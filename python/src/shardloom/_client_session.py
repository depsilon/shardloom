"""Deferred implementations for client session.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import client as _api


def ShardLoomClient___init__(self, binary, *, env, cwd, repo_root, profile_order, timeout, use_persistent_worker, transport, memory_gb, memory_bytes, max_parallelism, resources, resource_limits, float_policy, recursion_limit):
    from ._native_backend import normalize_float_policy, normalize_recursion_limit, normalize_transport, validate_timeout
    self._float_policy = normalize_float_policy(float_policy)
    self._recursion_limit = normalize_recursion_limit(recursion_limit)
    self._transport = normalize_transport(transport)
    if self._transport == "native" and use_persistent_worker is True:
        raise ValueError("a persistent subprocess worker cannot use transport='native'")
    self._native_backend = None
    self._native_backend_lock = _api.threading.Lock()
    self._native_closed = False
    self._resources = _api.optional_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
        resources=resources, limits=resource_limits,
    )
    self._resource_limits = _api.merge_resource_limits(
        resource_limits, self._resources.limits if self._resources is not None else None,
    )
    self._binary = binary
    self._env = dict(env) if env is not None else None
    self._cwd = _api.Path(cwd) if cwd is not None else None
    self._repo_root = _api.Path(repo_root) if repo_root is not None else None
    self._profile_order = tuple(profile_order)
    self._timeout = validate_timeout(timeout) if self._transport == "native" else timeout
    self._resolved_binary_cache: Binary | None = None
    self._use_persistent_worker = use_persistent_worker
    self._worker_process: subprocess.Popen[str] | None = None
    self._worker_stdout_buffer = b""
    self._worker_disabled = False
    self._worker_lock = _api.threading.Lock()
    self._active_processes: set[subprocess.Popen] = set()
    self._cancelled_processes: set[subprocess.Popen] = set()
    import weakref
    self._batch_streams = weakref.WeakSet()


def ShardLoomClient__native_backend_for(self, allocation):
    from ._native_backend import NativeBackend
    if self.transport != "native":
        raise ValueError("this operation requires transport='native'")
    if allocation is not None:
        _api.resolve_resources(
            resources=allocation, inherited=self.resources, limits=self.resource_limits,
        )
    with self._native_backend_lock:
        self._ensure_open()
        if self._native_backend is None:
            self._native_backend = NativeBackend(
                self.resources, self.resource_limits, self._timeout,
                float_policy=self.float_policy,
                recursion_limit=self.recursion_limit,
            )
        return self._native_backend


def ShardLoomClient_from_env(cls, env, *, profile_order, **kwargs):

    effective_env = dict(_api.os.environ)
    if env is not None:
        effective_env.update(env)
    repo_root = effective_env.get(_api.ENV_REPO_ROOT)
    configured_profile_order = profile_order or _api._profile_order_from_env(effective_env)
    timeout = kwargs.pop("timeout", _api._timeout_from_env(effective_env))
    return cls(
        env=effective_env,
        repo_root=repo_root,
        profile_order=configured_profile_order,
        timeout=timeout,
        **kwargs,
    )
