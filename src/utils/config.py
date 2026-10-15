"""Config loader — reads pipeline.yaml and exposes typed paths."""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import List
import yaml


@dataclass
class EmailAlertConfig:
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    sender_email: str = ""
    sender_password: str = ""
    recipients: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict) -> "EmailAlertConfig":
        return cls(
            smtp_server=raw.get("smtp_server", "smtp.gmail.com"),
            smtp_port=int(raw.get("smtp_port", 587)),
            sender_email=raw.get("sender_email", ""),
            sender_password=raw.get("sender_password", ""),
            recipients=raw.get("recipients", []),
        )


class Config:
    def __init__(self, raw: dict, root: Path):
        self._raw = raw
        self.root = root

    @classmethod
    def load(cls, path: str = "config/pipeline.yaml") -> "Config":
        p = Path(path)
        with p.open() as f:
            raw = yaml.safe_load(f)
        return cls(raw, p.parent.parent)

    def _p(self, key: str) -> Path:
        return self.root / self._raw["paths"][key]

    @property
    def landing_orders(self) -> Path:      return self._p("landing_orders")
    @property
    def landing_customers(self) -> Path:   return self._p("landing_customers")
    @property
    def landing_products_db(self) -> Path: return self._p("landing_products_db")
    @property
    def bronze(self) -> Path:              return self._p("bronze")
    @property
    def silver(self) -> Path:              return self._p("silver")
    @property
    def gold(self) -> Path:                return self._p("gold")
    @property
    def quarantine(self) -> Path:          return self._p("quarantine")
    @property
    def logs(self) -> Path:                return self._p("logs")
    @property
    def state(self) -> Path:               return self._p("state")
    @property
    def silver_cfg(self) -> dict:          return self._raw.get("silver", {})
    @property
    def gold_cfg(self) -> dict:            return self._raw.get("gold", {})
    @property
    def sources_cfg(self) -> dict:         return self._raw.get("sources", {})
    @property
    def email_cfg(self) -> EmailAlertConfig:
        return EmailAlertConfig.from_dict(self._raw.get("email_alerts", {}))
