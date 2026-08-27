"""schema_diff 纯逻辑单元测试：不连数据库，只验证 DDL 重建、差异判定与 SQL 渲染。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.schema_diff import (  # noqa: E402
    RISK_RISKY,
    RISK_SAFE,
    SCOPE_ALL,
    SCOPE_INTERSECTION,
    SCOPE_TARGET_EXISTING,
    Column,
    ForeignKey,
    Index,
    IndexPart,
    SchemaDiffError,
    Table,
    _rewrite_create_sql,
    diff_databases,
    load_instances,
    quote_ident,
    quote_literal,
    render_sql_script,
    summarize,
)


def col(name, ctype, *, pos=1, nullable=True, default=None, extra="", comment="",
        charset=None, collation=None, generated="", srs_id=None):
    return Column(
        name=name, position=pos, column_type=ctype, nullable=nullable, default=default,
        extra=extra, comment=comment, charset=charset, collation=collation,
        generation_expression=generated, srs_id=srs_id,
    )


def idx(name, columns, *, unique=False, index_type="BTREE", comment="", parser=None,
        sub_parts=None, desc=None, expressions=None):
    parts = []
    for i, column in enumerate(columns):
        parts.append(
            IndexPart(
                column=column,
                expression=(expressions or {}).get(column),
                sub_part=(sub_parts or {}).get(column),
                descending=bool((desc or {}).get(column)),
            )
        )
    return Index(name=name, unique=unique, index_type=index_type, comment=comment, parser=parser, parts=parts)


def table(name, columns=(), indexes=(), *, engine="InnoDB", collation="utf8mb4_0900_ai_ci",
          comment="", row_format="Dynamic", table_type="BASE TABLE", fks=()):
    return Table(
        name=name, table_type=table_type, engine=engine, collation=collation, comment=comment,
        row_format=row_format,
        columns={c.name: c for c in columns},
        indexes={i.name: i for i in indexes},
        foreign_keys={f.name: f for f in fks},
    )


class QuotingTest(unittest.TestCase):
    def test_ident_escapes_backtick(self):
        self.assertEqual(quote_ident("a`b"), "`a``b`")

    def test_literal_escapes_quote_and_backslash(self):
        self.assertEqual(quote_literal("it's"), "'it''s'")
        self.assertEqual(quote_literal("a\\b"), "'a\\\\b'")

    def test_literal_escapes_newline(self):
        self.assertEqual(quote_literal("a\nb"), "'a\\nb'")


class ColumnDefinitionTest(unittest.TestCase):
    def test_auto_increment_primary(self):
        c = col("id", "bigint unsigned", nullable=False, extra="auto_increment")
        self.assertEqual(c.definition(), "bigint unsigned NOT NULL AUTO_INCREMENT")

    def test_nullable_without_default(self):
        c = col("name", "varchar(255)", charset="utf8mb4", collation="utf8mb4_0900_ai_ci")
        self.assertEqual(
            c.definition(),
            "varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL",
        )

    def test_string_default_is_quoted(self):
        c = col("status", "varchar(16)", nullable=False, default="active")
        self.assertEqual(c.definition(), "varchar(16) NOT NULL DEFAULT 'active'")

    def test_empty_string_default_is_not_treated_as_missing(self):
        c = col("note", "varchar(16)", nullable=False, default="")
        self.assertEqual(c.definition(), "varchar(16) NOT NULL DEFAULT ''")

    def test_numeric_default_is_bare(self):
        c = col("qty", "int", nullable=False, default="0")
        self.assertEqual(c.definition(), "int NOT NULL DEFAULT 0")

    def test_current_timestamp_default_is_bare(self):
        c = col("created_at", "datetime", nullable=False, default="CURRENT_TIMESTAMP",
                extra="DEFAULT_GENERATED")
        self.assertEqual(c.definition(), "datetime NOT NULL DEFAULT CURRENT_TIMESTAMP")

    def test_current_timestamp_with_precision_and_on_update(self):
        c = col("updated_at", "datetime(3)", nullable=False, default="CURRENT_TIMESTAMP(3)",
                extra="DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)")
        self.assertEqual(
            c.definition(),
            "datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)",
        )

    def test_expression_default_gets_parentheses(self):
        c = col("uid", "varchar(36)", nullable=False, default="uuid()", extra="DEFAULT_GENERATED")
        self.assertEqual(c.definition(), "varchar(36) NOT NULL DEFAULT (uuid())")

    def test_generated_column_has_no_default(self):
        c = col("total", "int", generated="(`a` + `b`)", extra="STORED GENERATED")
        self.assertEqual(c.definition(), "int GENERATED ALWAYS AS ((`a` + `b`)) STORED NULL")

    def test_comment_is_escaped(self):
        c = col("x", "int", nullable=False, default="1", comment="it's ok")
        self.assertEqual(c.definition(), "int NOT NULL DEFAULT 1 COMMENT 'it''s ok'")

    def test_ddl_prefixes_quoted_name(self):
        c = col("order", "int", nullable=False, default="1")
        self.assertEqual(c.ddl(), "`order` int NOT NULL DEFAULT 1")


class IndexDefinitionTest(unittest.TestCase):
    def test_primary_key(self):
        self.assertEqual(idx("PRIMARY", ["id"], unique=True).definition(), "PRIMARY KEY (`id`)")

    def test_unique_key(self):
        self.assertEqual(idx("uk_a", ["a", "b"], unique=True).definition(), "UNIQUE KEY `uk_a` (`a`,`b`)")

    def test_plain_key(self):
        self.assertEqual(idx("ix_a", ["a"]).definition(), "KEY `ix_a` (`a`)")

    def test_prefix_and_desc(self):
        definition = idx("ix_p", ["a", "b"], sub_parts={"a": 10}, desc={"b": True}).definition()
        self.assertEqual(definition, "KEY `ix_p` (`a`(10),`b` DESC)")

    def test_functional_index(self):
        definition = idx("ix_f", ["c"], expressions={"c": "(json_extract(`d`,'$.x'))"}).definition()
        self.assertEqual(definition, "KEY `ix_f` (((json_extract(`d`,'$.x'))))")

    def test_fulltext_with_parser(self):
        definition = idx("ft", ["body"], index_type="FULLTEXT", parser="ngram").definition()
        self.assertEqual(definition, "FULLTEXT KEY `ft` (`body`) WITH PARSER `ngram`")

    def test_signature_ignores_name(self):
        left = idx("a", ["x"], unique=True)
        right = idx("b", ["x"], unique=True)
        self.assertEqual(left.signature(), right.signature())


class DiffTest(unittest.TestCase):
    def _diff(self, source, target, **kwargs):
        return diff_databases(
            {t.name: t for t in source}, {t.name: t for t in target},
            source_database="base_db", target_database="tgt_db", **kwargs,
        )

    def test_identical_tables_report_same(self):
        left = table("t", [col("id", "int", nullable=False)])
        right = table("t", [col("id", "int", nullable=False)])
        diffs = self._diff([left], [right])
        self.assertEqual(diffs[0].status, "same")
        self.assertEqual(diffs[0].changes, [])

    def test_missing_column_generates_add_with_after(self):
        left = table("t", [col("id", "int", pos=1, nullable=False), col("name", "varchar(20)", pos=2)])
        right = table("t", [col("id", "int", pos=1, nullable=False)])
        change = self._diff([left], [right])[0].changes[0]
        self.assertEqual(change.kind, "column_added")
        self.assertEqual(change.risk, RISK_SAFE)
        self.assertEqual(
            change.sql,
            "ALTER TABLE `tgt_db`.`t` ADD COLUMN `name` varchar(20) NULL DEFAULT NULL AFTER `id`;",
        )

    def test_first_column_missing_uses_first(self):
        left = table("t", [col("a", "int", pos=1), col("b", "int", pos=2)])
        right = table("t", [col("b", "int", pos=1)])
        change = self._diff([left], [right])[0].changes[0]
        self.assertIn(" FIRST;", change.sql)

    def test_widening_varchar_is_safe(self):
        left = table("t", [col("c", "varchar(255)", nullable=False)])
        right = table("t", [col("c", "varchar(64)", nullable=False)])
        change = self._diff([left], [right])[0].changes[0]
        self.assertEqual(change.kind, "column_modified")
        self.assertEqual(change.risk, RISK_SAFE)

    def test_narrowing_varchar_is_risky(self):
        left = table("t", [col("c", "varchar(64)", nullable=False)])
        right = table("t", [col("c", "varchar(255)", nullable=False)])
        self.assertEqual(self._diff([left], [right])[0].changes[0].risk, RISK_RISKY)

    def test_int_to_bigint_is_safe_but_reverse_is_risky(self):
        wide = table("t", [col("c", "bigint", nullable=False)])
        narrow = table("t", [col("c", "int", nullable=False)])
        self.assertEqual(self._diff([wide], [narrow])[0].changes[0].risk, RISK_SAFE)
        self.assertEqual(self._diff([narrow], [wide])[0].changes[0].risk, RISK_RISKY)

    def test_tightening_nullability_is_risky(self):
        left = table("t", [col("c", "int", nullable=False, default="0")])
        right = table("t", [col("c", "int", nullable=True)])
        self.assertEqual(self._diff([left], [right])[0].changes[0].risk, RISK_RISKY)

    def test_relaxing_nullability_is_safe(self):
        left = table("t", [col("c", "int", nullable=True)])
        right = table("t", [col("c", "int", nullable=False, default="0")])
        self.assertEqual(self._diff([left], [right])[0].changes[0].risk, RISK_SAFE)

    def test_collation_change_is_risky(self):
        left = table("t", [col("c", "varchar(10)", charset="utf8mb4", collation="utf8mb4_0900_ai_ci")])
        right = table("t", [col("c", "varchar(10)", charset="utf8mb4", collation="utf8mb4_general_ci")])
        self.assertEqual(self._diff([left], [right])[0].changes[0].risk, RISK_RISKY)

    def test_extra_column_in_target_is_risky_drop(self):
        left = table("t", [col("id", "int", nullable=False)])
        right = table("t", [col("id", "int", nullable=False), col("legacy", "int", pos=2)])
        change = self._diff([left], [right])[0].changes[0]
        self.assertEqual(change.kind, "column_dropped")
        self.assertEqual(change.risk, RISK_RISKY)
        self.assertEqual(change.sql, "ALTER TABLE `tgt_db`.`t` DROP COLUMN `legacy`;")

    def test_missing_index_is_safe_add(self):
        left = table("t", [col("a", "int")], [idx("ix_a", ["a"])])
        right = table("t", [col("a", "int")])
        change = self._diff([left], [right])[0].changes[0]
        self.assertEqual(change.kind, "index_added")
        self.assertEqual(change.sql, "ALTER TABLE `tgt_db`.`t` ADD KEY `ix_a` (`a`);")

    def test_changed_index_drops_then_adds(self):
        left = table("t", [col("a", "int"), col("b", "int")], [idx("ix", ["a", "b"])])
        right = table("t", [col("a", "int"), col("b", "int")], [idx("ix", ["a"])])
        change = self._diff([left], [right])[0].changes[0]
        self.assertEqual(change.kind, "index_modified")
        self.assertEqual(change.risk, RISK_RISKY)
        self.assertIn("DROP INDEX `ix`", change.sql)
        self.assertIn("ADD KEY `ix` (`a`,`b`)", change.sql)

    def test_primary_key_drop_uses_drop_primary_key(self):
        left = table("t", [col("a", "int", nullable=False)])
        right = table("t", [col("a", "int", nullable=False)], [idx("PRIMARY", ["a"], unique=True)])
        change = self._diff([left], [right])[0].changes[0]
        self.assertEqual(change.sql, "ALTER TABLE `tgt_db`.`t` DROP PRIMARY KEY;")

    def test_missing_table_emits_create(self):
        left = table("t", [col("id", "int", nullable=False)])
        left.create_sql = "CREATE TABLE `t` (\n  `id` int NOT NULL\n) ENGINE=InnoDB AUTO_INCREMENT=42 DEFAULT CHARSET=utf8mb4"
        diffs = self._diff([left], [])
        self.assertEqual(diffs[0].status, "missing_in_target")
        sql = diffs[0].changes[0].sql
        self.assertIn("CREATE TABLE IF NOT EXISTS `tgt_db`.`t`", sql)
        self.assertNotIn("AUTO_INCREMENT=42", sql)

    def test_extra_table_is_commented_out(self):
        right = table("legacy", [col("id", "int")])
        diffs = self._diff([], [right])
        self.assertEqual(diffs[0].status, "extra_in_target")
        self.assertTrue(diffs[0].changes[0].sql.lstrip().startswith("--"))

    def test_scope_target_existing_skips_missing_tables(self):
        left = table("only_in_base", [col("id", "int")])
        right = table("in_target", [col("id", "int")])
        names = {d.table for d in self._diff([left], [right], scope=SCOPE_TARGET_EXISTING)}
        self.assertEqual(names, {"in_target"})

    def test_scope_intersection_skips_both_sides(self):
        left = table("shared", [col("id", "int")])
        extra_left = table("only_base", [col("id", "int")])
        right = table("shared", [col("id", "int")])
        extra_right = table("only_target", [col("id", "int")])
        names = {d.table for d in self._diff([left, extra_left], [right, extra_right], scope=SCOPE_INTERSECTION)}
        self.assertEqual(names, {"shared"})

    def test_scope_all_covers_both_sides(self):
        left = table("only_base", [col("id", "int")])
        right = table("only_target", [col("id", "int")])
        names = {d.table for d in self._diff([left], [right], scope=SCOPE_ALL)}
        self.assertEqual(names, {"only_base", "only_target"})

    def test_views_skipped_by_default(self):
        view = table("v", [col("id", "int")], table_type="VIEW")
        self.assertEqual(self._diff([view], []), [])

    def test_table_option_diffs(self):
        left = table("t", [col("id", "int")], comment="基准注释", collation="utf8mb4_0900_ai_ci")
        right = table("t", [col("id", "int")], comment="旧注释", collation="utf8mb4_general_ci")
        kinds = {c.kind: c for c in self._diff([left], [right])[0].changes}
        self.assertEqual(kinds["option_comment"].risk, RISK_SAFE)
        self.assertEqual(kinds["option_collation"].risk, RISK_RISKY)
        self.assertIn("COLLATE utf8mb4_0900_ai_ci", kinds["option_collation"].sql)

    def test_foreign_key_add(self):
        fk = ForeignKey(name="fk_a", columns=["a_id"], referenced_schema="base_db",
                        referenced_table="a", referenced_columns=["id"],
                        update_rule="NO ACTION", delete_rule="CASCADE")
        left = table("t", [col("a_id", "int")], fks=[fk])
        right = table("t", [col("a_id", "int")])
        change = self._diff([left], [right])[0].changes[0]
        self.assertEqual(change.kind, "fk_added")
        self.assertIn("FOREIGN KEY (`a_id`) REFERENCES `a` (`id`) ON DELETE CASCADE", change.sql)


class RenderTest(unittest.TestCase):
    def _render(self, include_risky):
        left = table("t", [col("id", "int", nullable=False), col("name", "varchar(20)", pos=2)])
        right = table("t", [col("id", "int", nullable=False), col("gone", "int", pos=2)])
        diffs = diff_databases({left.name: left}, {right.name: right},
                              source_database="base_db", target_database="tgt_db")
        return render_sql_script(diffs, source_instance="test", source_database="base_db",
                                 target_instance="hub", target_database="tgt_db",
                                 include_risky=include_risky, generated_at="2026-08-06T00:00:00Z")

    def test_risky_statements_are_commented_by_default(self):
        script = self._render(False)
        self.assertIn("ADD COLUMN `name`", script)
        self.assertIn("-- ALTER TABLE `tgt_db`.`t` DROP COLUMN `gone`;", script)
        for line in script.splitlines():
            if "DROP COLUMN" in line:
                self.assertTrue(line.lstrip().startswith("--"), line)

    def test_risky_statements_expand_when_requested(self):
        script = self._render(True)
        self.assertIn("\nALTER TABLE `tgt_db`.`t` DROP COLUMN `gone`;", script)

    def test_header_names_both_sides(self):
        script = self._render(False)
        self.assertIn("基准：test.base_db", script)
        self.assertIn("目标：hub.tgt_db", script)

    def test_extra_tables_are_never_silently_dropped_from_script(self):
        """目标独有的表不生成 DDL，但必须在脚本里留痕，且计入高危统计。"""
        shared_source = table("shared", [col("id", "int", nullable=False)])
        shared_target = table("shared", [col("id", "int", nullable=False)])
        extra_target = table("legacy_only", [col("id", "int")])
        diffs = diff_databases(
            {shared_source.name: shared_source},
            {shared_target.name: shared_target, extra_target.name: extra_target},
            source_database="base_db", target_database="tgt_db",
        )
        script = render_sql_script(diffs, source_instance="test", source_database="base_db",
                                   target_instance="hub", target_database="tgt_db",
                                   include_risky=False, generated_at="")
        self.assertIn("legacy_only", script)
        self.assertIn("高危语句 1 条", script)
        for line in script.splitlines():
            if "legacy_only" in line:
                self.assertTrue(line.lstrip().startswith("--"), line)

    def test_summary_counts(self):
        left = table("t", [col("id", "int", nullable=False), col("name", "varchar(20)", pos=2)])
        right = table("t", [col("id", "int", nullable=False)])
        other = table("only_target", [col("id", "int")])
        diffs = diff_databases({left.name: left}, {right.name: right, other.name: other},
                               source_database="base_db", target_database="tgt_db")
        summary = summarize(diffs)
        self.assertEqual(summary["changed"], 1)
        self.assertEqual(summary["extraInTarget"], 1)
        self.assertEqual(summary["safeChanges"], 1)
        self.assertEqual(summary["kinds"]["column_added"], 1)


class DefaultCompareTest(unittest.TestCase):
    """页面默认选中的基准/目标/范围来自配置，不写死在前端。"""

    def _write(self, tmp, payload):
        import json

        path = Path(tmp) / "instances.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return {"RDS_BINLOG_SCHEMA_CONFIG": str(path)}

    def test_reads_default_compare_from_json(self):
        import tempfile

        from app.schema_diff import load_default_compare

        payload = {
            "instances": [
                {"name": "test", "host": "h1", "user": "u"},
                {"name": "hub", "host": "h2", "user": "u"},
            ],
            "defaultCompare": {
                "sourceInstance": "test", "sourceDatabase": "example_source",
                "targetInstance": "hub", "targetDatabase": "example_target",
                "scope": "both",
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            default = load_default_compare(self._write(tmp, payload))
        self.assertEqual(default["sourceInstance"], "test")
        self.assertEqual(default["sourceDatabase"], "example_source")
        self.assertEqual(default["targetInstance"], "hub")
        self.assertEqual(default["targetDatabase"], "example_target")
        self.assertEqual(default["scope"], "both")

    def test_missing_default_returns_empty(self):
        import tempfile

        from app.schema_diff import load_default_compare

        payload = {"instances": [{"name": "test", "host": "h", "user": "u"}]}
        with tempfile.TemporaryDirectory() as tmp:
            default = load_default_compare(self._write(tmp, payload))
        self.assertEqual(default["sourceInstance"], "")
        self.assertEqual(default["scope"], "")

    def test_invalid_scope_is_dropped(self):
        import tempfile

        from app.schema_diff import load_default_compare

        payload = {
            "instances": [{"name": "test", "host": "h", "user": "u"}],
            "defaultCompare": {"sourceInstance": "test", "scope": "rm -rf"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            default = load_default_compare(self._write(tmp, payload))
        self.assertEqual(default["scope"], "")
        self.assertEqual(default["sourceInstance"], "test")

    def test_illegal_database_name_is_dropped(self):
        import tempfile

        from app.schema_diff import load_default_compare

        payload = {
            "instances": [{"name": "test", "host": "h", "user": "u"}],
            "defaultCompare": {"sourceDatabase": "a`b; DROP", "targetDatabase": "example_target"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            default = load_default_compare(self._write(tmp, payload))
        self.assertEqual(default["sourceDatabase"], "")
        self.assertEqual(default["targetDatabase"], "example_target")

    def test_no_config_returns_empty_default(self):
        from app.schema_diff import load_default_compare

        self.assertEqual(load_default_compare({})["scope"], "")

    def test_service_exposes_default_compare(self):
        from app.schema_diff import InstanceConfig, SchemaDiffService

        service = SchemaDiffService(
            [InstanceConfig(name="test", label="secondary-db", instance_id="rm-a", host="h", port=3306, user="u")],
            default_compare={"sourceInstance": "test", "sourceDatabase": "example_source",
                             "targetInstance": "hub", "targetDatabase": "example_target", "scope": "both"},
        )
        self.assertEqual(service.default_compare()["scope"], "both")
        self.assertEqual(service.default_compare()["targetDatabase"], "example_target")


class DisplayNameTest(unittest.TestCase):
    """实例显示名以 RDS 控制台名称(DBInstanceDescription)为准，取不到才回退配置名。"""

    def _service(self, resolver):
        from app.schema_diff import InstanceConfig, SchemaDiffService

        instances = [
            InstanceConfig(name="prod", label="配置里的名字", instance_id="rm-aaa", host="h1", port=3306, user="u"),
            InstanceConfig(name="hub", label="配置里的名字2", instance_id="rm-bbb", host="h2", port=3306, user="u"),
            InstanceConfig(name="local", label="无实例ID", instance_id="", host="h3", port=3306, user="u"),
        ]
        return SchemaDiffService(instances, name_resolver=resolver)

    def test_uses_rds_name_when_available(self):
        service = self._service(lambda instance_id: {"rm-aaa": "primary-db", "rm-bbb": "target-db"}.get(instance_id))
        labels = {i["name"]: i["label"] for i in service.instances()}
        self.assertEqual(labels["prod"], "primary-db")
        self.assertEqual(labels["hub"], "target-db")

    def test_falls_back_to_config_label_when_resolver_returns_nothing(self):
        service = self._service(lambda instance_id: None)
        labels = {i["name"]: i["label"] for i in service.instances()}
        self.assertEqual(labels["prod"], "配置里的名字")

    def test_falls_back_when_resolver_raises(self):
        def boom(instance_id):
            raise RuntimeError("RAM 权限不足")

        service = self._service(boom)
        labels = {i["name"]: i["label"] for i in service.instances()}
        self.assertEqual(labels["prod"], "配置里的名字")
        self.assertEqual(labels["hub"], "配置里的名字2")

    def test_instance_without_id_is_not_queried(self):
        seen = []

        def resolver(instance_id):
            seen.append(instance_id)
            return "x"

        service = self._service(resolver)
        service.instances()
        self.assertNotIn("", seen)
        self.assertEqual(sorted(seen), ["rm-aaa", "rm-bbb"])

    def test_resolver_called_once_per_instance_across_calls(self):
        calls = []

        def resolver(instance_id):
            calls.append(instance_id)
            return "name-" + instance_id

        service = self._service(resolver)
        service.instances()
        service.instances()
        service.instances()
        self.assertEqual(len(calls), 2)

    def test_no_resolver_keeps_config_label(self):
        service = self._service(None)
        labels = {i["name"]: i["label"] for i in service.instances()}
        self.assertEqual(labels["hub"], "配置里的名字2")

    def test_rds_name_also_used_in_compare_error_free_path(self):
        service = self._service(lambda instance_id: "target-db" if instance_id == "rm-bbb" else "primary-db")
        payload = service.instances()
        self.assertTrue(all("label" in item and item["label"] for item in payload))
        self.assertNotIn("配置里的名字", [item["label"] for item in payload])


class RewriteCreateTest(unittest.TestCase):
    def test_rewrites_schema_and_drops_auto_increment(self):
        sql = "CREATE TABLE `orders` (\n  `id` bigint NOT NULL\n) ENGINE=InnoDB AUTO_INCREMENT=99 DEFAULT CHARSET=utf8mb4"
        result = _rewrite_create_sql(sql, "orders", "target_db")
        self.assertTrue(result.startswith("CREATE TABLE IF NOT EXISTS `target_db`.`orders` ("))
        self.assertNotIn("AUTO_INCREMENT=99", result)
        self.assertTrue(result.endswith(";"))

    def test_rejects_non_create_statement(self):
        with self.assertRaises(SchemaDiffError):
            _rewrite_create_sql("SELECT 1", "t", "db")


class LoadInstancesTest(unittest.TestCase):
    def test_empty_when_not_declared(self):
        self.assertEqual(load_instances({}), [])

    def test_parses_declared_instances(self):
        env = {
            "RDS_BINLOG_SCHEMA_INSTANCES": "prod,hub",
            "RDS_BINLOG_SCHEMA_PROD_HOST": "h1", "RDS_BINLOG_SCHEMA_PROD_USER": "readonly_user",
            "RDS_BINLOG_SCHEMA_HUB_HOST": "h2", "RDS_BINLOG_SCHEMA_HUB_USER": "readonly_user",
            "RDS_BINLOG_SCHEMA_HUB_PORT": "3307", "RDS_BINLOG_SCHEMA_HUB_LABEL": "sync-target",
        }
        instances = load_instances(env)
        self.assertEqual([i.name for i in instances], ["prod", "hub"])
        self.assertEqual(instances[1].port, 3307)
        self.assertEqual(instances[1].label, "sync-target")

    def test_missing_host_raises(self):
        with self.assertRaises(SchemaDiffError):
            load_instances({"RDS_BINLOG_SCHEMA_INSTANCES": "x", "RDS_BINLOG_SCHEMA_X_USER": "u"})

    def test_loads_from_json_config_file(self):
        import json
        import tempfile

        payload = {
            "instances": [
                {"name": "prod", "label": "primary-db", "instanceId": "rm-1", "host": "h1",
                 "port": 3306, "user": "readonly_user", "password": "p#a$s w'o\"rd"},
                {"name": "hub", "host": "h2", "user": "readonly_user", "password": "x"},
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "instances.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            instances = load_instances({"RDS_BINLOG_SCHEMA_CONFIG": str(path)})
        self.assertEqual([i.name for i in instances], ["prod", "hub"])
        self.assertEqual(instances[0].password, "p#a$s w'o\"rd")
        self.assertEqual(instances[0].label, "primary-db")
        self.assertEqual(instances[1].label, "hub")
        self.assertEqual(instances[1].port, 3306)

    def test_json_config_missing_file_raises(self):
        with self.assertRaises(SchemaDiffError):
            load_instances({"RDS_BINLOG_SCHEMA_CONFIG": "/nonexistent/schema-instances.json"})

    def test_json_config_rejects_bad_name(self):
        import json
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.json"
            path.write_text(json.dumps({"instances": [{"name": "a b", "host": "h", "user": "u"}]}), encoding="utf-8")
            with self.assertRaises(SchemaDiffError):
                load_instances({"RDS_BINLOG_SCHEMA_CONFIG": str(path)})

    def test_password_not_in_repr(self):
        env = {
            "RDS_BINLOG_SCHEMA_INSTANCES": "prod",
            "RDS_BINLOG_SCHEMA_PROD_HOST": "h", "RDS_BINLOG_SCHEMA_PROD_USER": "u",
            "RDS_BINLOG_SCHEMA_PROD_PASSWORD": "SuperSecret123",
        }
        instance = load_instances(env)[0]
        self.assertNotIn("SuperSecret123", repr(instance))
        self.assertNotIn("SuperSecret123", str(instance.public_dict()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
