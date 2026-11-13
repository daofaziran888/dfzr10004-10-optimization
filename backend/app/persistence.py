"""去重批次状态的轻量持久化。

水位、分段进度、最近结果、结果快照与复核清单都要跨进程重启保留下来，
所以不放在内存字典里，而是落到一个 JSON 文件。真实项目里换成数据库表即可，
这里只依赖标准库，保证克隆下来就能用。
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

STATE_ENV = "DEDUP_STATE_PATH"
DEFAULT_STATE_PATH = Path(__file__).resolve().parent.parent / "var" / "dedup_state.json"


def state_path() -> Path:
    override = os.environ.get(STATE_ENV)
    if override:
        return Path(override).expanduser()
    return DEFAULT_STATE_PATH


class JsonState:
    """带读写锁的 JSON 状态文件；写入采用临时文件 + 原子替换。

    路径在每次访问时解析（见 `path` 属性），这样启动时读取的环境变量
    与运行中切换的路径都能生效；`data` 是内存镜像。
    """

    def __init__(self, override: Path | None = None) -> None:
        self._override = override
        self.lock = threading.RLock()
        self.data: dict[str, Any] = self._load()

    @property
    def path(self) -> Path:
        return self._override or state_path()

    @path.setter
    def path(self, value: Path | None) -> None:
        self._override = value
        with self.lock:
            self.data = self._load()

    def reload(self) -> dict[str, Any]:
        """从磁盘重新读取状态；进程内模拟重启或外部改动时使用。"""
        with self.lock:
            self.data = self._load()
            return self.data

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            # 状态文件损坏时不阻断服务启动；下一次写入会覆盖成合法结构。
            return {}
        return payload if isinstance(payload, dict) else {}

    def save(self) -> None:
        with self.lock:
            if self.path is None:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(self.data, ensure_ascii=False, sort_keys=True, indent=2)
            fd, tmp_name = tempfile.mkstemp(prefix=".dedup-", suffix=".tmp", dir=str(self.path.parent))
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(payload)
                os.replace(tmp_name, self.path)
            finally:
                if os.path.exists(tmp_name):
                    os.unlink(tmp_name)
