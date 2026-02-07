"""Grok model registry."""

from __future__ import annotations

from enum import Enum
from typing import List, Optional, Tuple

from pydantic import BaseModel, Field

from app.core.exceptions import ValidationException


class Tier(str, Enum):
    BASIC = "basic"
    SUPER = "super"


class Cost(str, Enum):
    LOW = "low"
    HIGH = "high"


class ModelInfo(BaseModel):
    model_id: str
    grok_model: str
    rate_limit_model: str
    model_mode: str
    tier: Tier = Field(default=Tier.BASIC)
    cost: Cost = Field(default=Cost.LOW)
    display_name: str
    description: str = ""
    is_video: bool = False
    is_image: bool = False


class ModelService:
    MODELS = [
        ModelInfo(model_id="grok-3", grok_model="grok-3", rate_limit_model="grok-3", model_mode="MODEL_MODE_AUTO", display_name="Grok 3"),
        ModelInfo(model_id="grok-3-fast", grok_model="grok-3", rate_limit_model="grok-3", model_mode="MODEL_MODE_FAST", display_name="Grok 3 Fast"),
        ModelInfo(model_id="grok-3-mini", grok_model="grok-3", rate_limit_model="grok-3", model_mode="MODEL_MODE_GROK_3_MINI_THINKING", display_name="Grok 3 Mini"),
        ModelInfo(model_id="grok-3-thinking", grok_model="grok-3", rate_limit_model="grok-3", model_mode="MODEL_MODE_GROK_3_THINKING", cost=Cost.HIGH, display_name="Grok 3 Thinking"),

        ModelInfo(model_id="grok-4", grok_model="grok-4", rate_limit_model="grok-4", model_mode="MODEL_MODE_AUTO", display_name="Grok 4"),
        ModelInfo(model_id="grok-4-fast", grok_model="grok-4", rate_limit_model="grok-4", model_mode="MODEL_MODE_FAST", display_name="Grok 4 Fast"),
        ModelInfo(model_id="grok-4-mini", grok_model="grok-4-mini-thinking-tahoe", rate_limit_model="grok-4-mini-thinking-tahoe", model_mode="MODEL_MODE_GROK_4_MINI_THINKING", display_name="Grok 4 Mini"),
        ModelInfo(model_id="grok-4-thinking", grok_model="grok-4", rate_limit_model="grok-4", model_mode="MODEL_MODE_GROK_4_THINKING", cost=Cost.HIGH, display_name="Grok 4 Thinking"),
        ModelInfo(model_id="grok-4-heavy", grok_model="grok-4", rate_limit_model="grok-4-heavy", model_mode="MODEL_MODE_HEAVY", tier=Tier.SUPER, cost=Cost.HIGH, display_name="Grok 4 Heavy"),

        ModelInfo(model_id="grok-4.1", grok_model="grok-4-1-thinking-1129", rate_limit_model="grok-4-1-thinking-1129", model_mode="MODEL_MODE_AUTO", display_name="Grok 4.1"),
        ModelInfo(model_id="grok-4.1-mini", grok_model="grok-4-1-thinking-1129", rate_limit_model="grok-4-1-thinking-1129", model_mode="MODEL_MODE_GROK_4_1_MINI_THINKING", display_name="Grok 4.1 Mini"),
        ModelInfo(model_id="grok-4.1-fast", grok_model="grok-4-1-thinking-1129", rate_limit_model="grok-4-1-thinking-1129", model_mode="MODEL_MODE_FAST", display_name="Grok 4.1 Fast"),
        ModelInfo(model_id="grok-4.1-expert", grok_model="grok-4-1-thinking-1129", rate_limit_model="grok-4-1-thinking-1129", model_mode="MODEL_MODE_EXPERT", display_name="Grok 4.1 Expert"),
        ModelInfo(model_id="grok-4.1-thinking", grok_model="grok-4-1-thinking-1129", rate_limit_model="grok-4-1-thinking-1129", model_mode="MODEL_MODE_GROK_4_1_THINKING", cost=Cost.HIGH, display_name="Grok 4.1 Thinking"),

        ModelInfo(model_id="grok-imagine-1.0", grok_model="grok-3", rate_limit_model="grok-3", model_mode="MODEL_MODE_FAST", cost=Cost.HIGH, display_name="Grok Image", description="Image generation model", is_image=True),
        ModelInfo(model_id="grok-imagine-1.0-edit", grok_model="imagine-image-edit", rate_limit_model="grok-3", model_mode="MODEL_MODE_FAST", cost=Cost.HIGH, display_name="Grok Image Edit", description="Image edit model", is_image=True),
        ModelInfo(model_id="grok-imagine-1.0-video", grok_model="grok-3", rate_limit_model="grok-3", model_mode="MODEL_MODE_FAST", cost=Cost.HIGH, display_name="Grok Video", description="Video generation model", is_video=True),
    ]

    _map = {m.model_id: m for m in MODELS}

    @classmethod
    def get(cls, model_id: str) -> Optional[ModelInfo]:
        return cls._map.get(model_id)

    @classmethod
    def list(cls) -> list[ModelInfo]:
        return list(cls._map.values())

    @classmethod
    def valid(cls, model_id: str) -> bool:
        return model_id in cls._map

    @classmethod
    def to_grok(cls, model_id: str) -> Tuple[str, str]:
        model = cls.get(model_id)
        if not model:
            raise ValidationException(f"Invalid model ID: {model_id}")
        return model.grok_model, model.model_mode

    @classmethod
    def rate_limit_model_for(cls, model_id: str) -> str:
        model = cls.get(model_id)
        return model.rate_limit_model if model else model_id

    @classmethod
    def is_heavy_bucket_model(cls, model_id: str) -> bool:
        return model_id == "grok-4-heavy"

    @classmethod
    def pool_for_model(cls, model_id: str) -> str:
        model = cls.get(model_id)
        if model and model.tier == Tier.SUPER:
            return "ssoSuper"
        return "ssoBasic"

    @classmethod
    def pool_candidates_for_model(cls, model_id: str) -> List[str]:
        model = cls.get(model_id)
        if model and model.tier == Tier.SUPER:
            return ["ssoSuper"]
        return ["ssoBasic", "ssoSuper"]


__all__ = ["ModelService", "ModelInfo", "Tier", "Cost"]
