"""Offline tests for intelligent screening active & per-message LLM metadata.

Uses isolated stdlib stubs and AST checks so it runs without FastAPI, Pydantic,
or Motor installed in the offline test runner.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parents[2]
V2_PATH = REPO_ROOT / "app/services/intelligent_screening_v2.py"
V1_PATH = REPO_ROOT / "app/services/intelligent_screening_service.py"
SCHEMAS_PATH = REPO_ROOT / "app/schemas/intelligent_screening.py"
ROUTER_PATH = REPO_ROOT / "app/routers/intelligent_screening.py"


def _load_isolated_v2_module():
    def _selection_model_name(sel):
        return (sel or {}).get("model_name")

    def _select_llm_config_from_doc(config_doc, *, config_id=None, model_name=None):
        for cfg in (config_doc or {}).get("llm_configs") or []:
            if model_name and cfg.get("model_name") == model_name:
                return cfg
        return None

    def _resolve_models(config_doc):
        ss = (config_doc or {}).get("system_settings") or {}
        dm = (config_doc or {}).get("default_models") or {}

        def pick(key):
            name = dm.get(key) or ss.get(key)
            if not name:
                return None
            matched = _select_llm_config_from_doc(config_doc, model_name=name)
            return {
                "model_name": name,
                "provider": (matched or {}).get("provider"),
                "config_id": (matched or {}).get("config_id"),
            }

        return pick("quick_analysis_model"), pick("deep_analysis_model"), pick("deep_reasoning_model"), pick("coding_model")

    ia_mod = types.ModuleType("app.services.intelligent_assistant_service")
    ia_mod._get_assistant_llm_config = AsyncMock(return_value=None)
    ia_mod._get_latest_trade_date = lambda: "2026-11-26"
    ia_mod._resolve_models = _resolve_models
    ia_mod._select_llm_config_from_doc = _select_llm_config_from_doc
    ia_mod._selection_model_name = _selection_model_name
    ia_mod.ASSISTANT_TIMEOUT_SECONDS = 900

    fr_mod = types.ModuleType("app.services.factor_registry_service")
    fr_mod.build_screening_fields_markdown = lambda **k: ""

    smd_mod = types.ModuleType("app.services.screening_metric_display")
    smd_mod.resolve_metric_value = lambda *a, **k: None
    smd_mod.select_metric_display_spec = lambda *a, **k: ("pe", "PE")

    sst_mod = types.ModuleType("core.tools.implementations.market.stock_screening_tool")
    sst_mod._execute_screening_query = AsyncMock(return_value=([], 0))
    sst_mod._format_screening_output = lambda *a, **k: ""

    core_tools = types.ModuleType("core.tools")
    core_tools.get_tool_registry = lambda: None
    core_tools.tool_metadata_list_to_openai = lambda x: []
    core_tools.__path__ = []
    core_tools_ctx = types.ModuleType("core.tools.context")
    core_tools_ctx.set_current_user_id = lambda x: None
    core_tools_impl = types.ModuleType("core.tools.implementations")
    core_tools_impl.__path__ = []
    core_tools_mkt = types.ModuleType("core.tools.implementations.market")
    core_tools_mkt.__path__ = []

    core_llm = types.ModuleType("core.llm")
    core_llm.UnifiedLLMClient = object
    core_llm.__path__ = []
    core_llm_models = types.ModuleType("core.llm.models")
    core_llm_models.Message = object
    core_llm_models.MessageRole = types.SimpleNamespace(SYSTEM="system", USER="user", ASSISTANT="assistant")

    motor_mod = types.ModuleType("motor")
    motor_mod.__path__ = []
    motor_asyncio = types.ModuleType("motor.motor_asyncio")
    motor_asyncio.AsyncIOMotorDatabase = object

    stubs = {
        "motor": motor_mod,
        "motor.motor_asyncio": motor_asyncio,
        "app": types.ModuleType("app"),
        "app.services": types.ModuleType("app.services"),
        "app.services.intelligent_assistant_service": ia_mod,
        "app.services.factor_registry_service": fr_mod,
        "app.services.screening_metric_display": smd_mod,
        "core": types.ModuleType("core"),
        "core.tools": core_tools,
        "core.tools.context": core_tools_ctx,
        "core.tools.implementations": core_tools_impl,
        "core.tools.implementations.market": core_tools_mkt,
        "core.tools.implementations.market.stock_screening_tool": sst_mod,
        "core.llm": core_llm,
        "core.llm.models": core_llm_models,
    }
    stubs["app"].__path__ = []
    stubs["app.services"].__path__ = []
    stubs["core"].__path__ = []

    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("_offline_screening_v2", V2_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


class _FakeSystemConfigs:
    def __init__(self, doc):
        self._doc = doc

    async def find_one(self, *args, **kwargs):
        return self._doc


class _FakeConversations:
    def __init__(self):
        self.last_update = None

    async def update_one(self, filter_q, update_q, upsert=False):
        self.last_update = (filter_q, update_q, upsert)


class _FakeDB:
    def __init__(self, config_doc=None):
        self.system_configs = _FakeSystemConfigs(config_doc)
        self.screening_conversations = _FakeConversations()


class IntelligentScreeningModelInfoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.v2 = _load_isolated_v2_module()

    def test_resolve_screening_llm_meta_returns_provider_model_source_without_secrets(self):
        config_doc = {
            "is_active": True,
            "default_llm": "qwen3-max",
            "system_settings": {"deep_analysis_model": "deepseek-v4-pro"},
            "llm_configs": [
                {"provider": "ai42", "model_name": "deepseek-v4-pro", "api_key": "secret-should-not-leak", "enabled": True}
            ],
        }
        fake_db = _FakeDB(config_doc=config_doc)
        resolved_cfg = types.SimpleNamespace(provider="dashscope", model="deepseek-v4-pro", api_key="secret-should-not-leak")

        with patch.object(self.v2, "_get_assistant_llm_config", AsyncMock(return_value=resolved_cfg)):
            meta = asyncio.run(self.v2.resolve_screening_llm_meta(fake_db))

        self.assertTrue(meta["configured"])
        self.assertEqual(meta["llm_provider"], "ai42")
        self.assertEqual(meta["llm_model"], "deepseek-v4-pro")
        self.assertEqual(meta["llm_source"], "深度分析模型")
        self.assertNotIn("api_key", meta)

    def test_resolve_screening_llm_meta_when_unconfigured(self):
        fake_db = _FakeDB(config_doc=None)
        with patch.object(self.v2, "_get_assistant_llm_config", AsyncMock(return_value=None)):
            meta = asyncio.run(self.v2.resolve_screening_llm_meta(fake_db))

        self.assertFalse(meta["configured"])
        self.assertIsNone(meta["llm_provider"])
        self.assertIsNone(meta["llm_model"])
        self.assertIsNone(meta["llm_source"])

    def test_save_message_persists_llm_fields(self):
        fake_db = _FakeDB()
        service = self.v2.IntelligentScreeningServiceV2(fake_db)
        service._llm_meta = {
            "configured": True,
            "llm_provider": "dashscope",
            "llm_model": "qwen3-max",
            "llm_source": "系统默认模型",
        }
        asyncio.run(
            service._save_message(
                "u1",
                "conv-1",
                "帮我选低估值银行股",
                "为您分析需求",
                ["screen_stocks_by_criteria"],
                [],
                "confirmation",
            )
        )
        self.assertIsNotNone(fake_db.screening_conversations.last_update)
        _, update_q, _ = fake_db.screening_conversations.last_update
        assistant_entry = update_q["$push"]["messages"]["$each"][1]
        self.assertEqual(assistant_entry["llm_provider"], "dashscope")
        self.assertEqual(assistant_entry["llm_model"], "qwen3-max")
        self.assertEqual(assistant_entry["llm_source"], "系统默认模型")

    def test_schemas_router_and_v1_expose_llm_model_fields(self):
        schema_src = SCHEMAS_PATH.read_text(encoding="utf-8")
        router_src = ROUTER_PATH.read_text(encoding="utf-8")
        v1_src = V1_PATH.read_text(encoding="utf-8")

        for field in ("llm_provider", "llm_model", "llm_source"):
            self.assertIn(field, schema_src)
            self.assertIn(field, router_src)
            self.assertIn(field, v1_src)

        self.assertIn("class ScreeningModelInfoResponse", schema_src)
        self.assertIn('"/model-info"', router_src)
        self.assertIn("resolve_screening_llm_meta", router_src)


if __name__ == "__main__":
    unittest.main()
