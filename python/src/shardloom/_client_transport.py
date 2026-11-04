"""Deferred implementations for client transport.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import client as _api


def ShardLoomClient_smoke_check(self, *, check):

    binary_command = tuple(self._binary_parts())
    return _api.PythonClientSmokeReport(
        binary_command=binary_command,
        python_package_version=_api.__version__,
        platform=_api.platform.platform(),
        status=self.status(check=check),
        python_capabilities=self.capabilities("python", check=check),
        deployment_capabilities=self.capabilities("deployment", check=check),
        input_adapters=self.input_adapters(check=check),
    )


def ShardLoomClient_run(self, args, *, check):

    self._ensure_open()
    if any(str(arg) == "--float-policy" or str(arg).startswith("--float-policy=") for arg in args):
        raise ValueError("--float-policy is fixed by the ShardLoomClient configuration")
    if self.transport == "native":
        command = [str(arg) for arg in args]
        payload = self._native_backend_for().command(command)
        envelope = _api.OutputEnvelope.from_json(_api.json.loads(payload))
        if check and envelope.is_error:
            raise _api.ShardLoomCommandError(("shardloom._native", *command), 2, envelope, "")
        return envelope
    if self._worker_enabled():
        try:
            return self._run_worker(args, check=check)
        except _api._ShardLoomWorkerStartupError:
            self._worker_disabled = True
            self._close_worker()
            return self._run_subprocess(args, check=check)
    return self._run_subprocess(args, check=check)


def ShardLoomClient__stop_active_work(self, *, closing):
    errors = []
    with self._native_backend_lock:
        if closing:
            self._native_closed = True
        else:
            self._ensure_open()
        if self._native_backend is not None:
            try:
                if closing:
                    self._native_backend.close()
                else:
                    self._native_backend.cancel()
            except Exception as error:
                errors.append(error)
        processes = tuple(self._active_processes)
        self._cancelled_processes.update(processes)
        streams = tuple(self._batch_streams)
        # An active worker's caller owns its pipes and reaping. Closing a
        # TextIOWrapper from here can block behind its reader's lock.
        worker_active = self._worker_process in self._active_processes
    for stream in streams:
        try:
            stream.cancel()
        except Exception as error:
            errors.append(error)
    for process in processes:
        try:
            if process.poll() is None:
                process.kill()
        except ProcessLookupError:
            pass  # The caller still owns the completed process and reaps it.
        except Exception as error:
            errors.append(error)
    if closing and not worker_active:
        self._close_worker()
    if errors:
        raise errors[0]


def ShardLoomClient__run_subprocess(self, args, *, check):
    command = self._command(args)
    redacted_command = _api._redact_command_for_error(command)
    try:
        with self._native_backend_lock:
            self._ensure_open()
            process = _api.subprocess.Popen(
                command, cwd=self._cwd, env=self._effective_env(), text=True,
                stdout=_api.subprocess.PIPE, stderr=_api.subprocess.PIPE,
            )
            self._active_processes.add(process)
    except FileNotFoundError as exc:
        raise _api.ShardLoomBinaryNotFoundError(
            "ShardLoom CLI binary was not found while running "
            f"{command[0]!r}. Install the ShardLoom CLI package, put "
            "`shardloom` on PATH, or set SHARDLOOM_BIN to a valid binary."
        ) from exc
    try:
        try:
            stdout, stderr = process.communicate(timeout=self._timeout)
        except _api.subprocess.TimeoutExpired as error:
            process.kill()
            process.communicate()
            raise _api.ShardLoomTimeoutError(
                redacted_command, self._timeout, error.output, error.stderr,
            ) from error
        except BaseException:
            # KeyboardInterrupt, cancellation and communication failures
            # must not leave a detached child running an output operation.
            if process.poll() is None:
                process.kill()
            process.communicate()
            raise
        self._check_process_cancelled(process, redacted_command)
        envelope = self._parse_stdout(stdout, redacted_command)
        if check and (process.returncode != 0 or envelope.is_error):
            raise _api.ShardLoomCommandError(redacted_command, process.returncode, envelope, stderr)
        return envelope
    finally:
        self._release_process(process)
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()


def ShardLoomClient__run_worker(self, args, *, check):
    command_args = self._command_args(args)
    redacted_command = _api._redact_command_for_error(
        [*self._binary_parts(), *command_args]
    )
    with self._worker_lock:
        self._ensure_open()
        process = self._ensure_worker()
        if process.stdin is None or process.stdout is None:
            self._worker_disabled = True
            raise _api._ShardLoomWorkerStartupError("worker pipes were not available")
        request = _api.json.dumps({"args": command_args}, separators=(",", ":"))
        deadline = (
            _api.time.monotonic() + self._timeout
            if self._timeout is not None and _api.os.name != "nt"
            else None
        )
        try:
            self._register_process(process)
            try:
                # From this point on the request may have reached the engine.
                # A missing response never authorizes replay, even on call one.
                self._write_worker_request(process, request + "\n", redacted_command, deadline)
                response_line = self._read_worker_response_line(process, redacted_command, deadline)
                self._check_process_cancelled(process, redacted_command)
                if not response_line:
                    raise _api.ShardLoomProtocolError(
                        "ShardLoom worker exited without a response; execution may have begun. No request was replayed."
                    )
                envelope = self._parse_stdout(response_line, redacted_command)
                expected_command = command_args[0] if command_args else ""
                if expected_command and envelope.command == "python-worker":
                    raise _api.ShardLoomProtocolError(
                        "ShardLoom worker returned a startup envelope instead of the requested command; no request was replayed. "
                        "For an older CLI, explicitly configure use_persistent_worker=False."
                    )
            except _api.ShardLoomTimeoutError:
                # The compatibility timeout also inherits OSError through
                # TimeoutError; preserve its type and structured diagnostic.
                raise
            except (BrokenPipeError, OSError, ValueError) as exc:
                self._check_process_cancelled(process, redacted_command)
                raise _api.ShardLoomProtocolError(
                    "ShardLoom worker communication failed after request admission; no request was replayed."
                ) from exc
            if check and envelope.is_error:
                raise _api.ShardLoomCommandError(redacted_command, 2, envelope, "")
            return envelope
        except BaseException:
            self._close_worker()
            raise
        finally:
            self._release_process(process)


def ShardLoomClient__write_worker_request_with_timeout(self, process, payload, redacted_command, deadline):
    if process.stdin is None:
        return
    fd = process.stdin.fileno()
    data = payload.encode("utf-8")
    view = memoryview(data)
    offset = 0
    selector = _api.selectors.DefaultSelector()
    previous_blocking = _api.os.get_blocking(fd)
    timed_out = False
    try:
        _api.os.set_blocking(fd, False)
        selector.register(fd, _api.selectors.EVENT_WRITE)
        while offset < len(view):
            remaining = deadline - _api.time.monotonic()
            if remaining <= 0:
                timed_out = True
                raise _api.ShardLoomTimeoutError(redacted_command, self._timeout)
            try:
                written = _api.os.write(fd, view[offset:])
            except BlockingIOError:
                written = 0
            if written:
                offset += written
                continue
            events = selector.select(remaining)
            if not events:
                timed_out = True
                raise _api.ShardLoomTimeoutError(redacted_command, self._timeout)
    finally:
        selector.close()
        try:
            _api.os.set_blocking(fd, previous_blocking)
        except OSError:
            pass
        if timed_out:
            self._close_worker()


def ShardLoomClient__read_worker_response_line(self, process, redacted_command, deadline):
    if process.stdout is None:
        return ""
    if deadline is None:
        buffered_line = self._pop_worker_stdout_buffered_line()
        if buffered_line is not None:
            return buffered_line
        if self._worker_stdout_buffer:
            prefix = self._decode_worker_stdout(self._worker_stdout_buffer)
            self._worker_stdout_buffer = b""
            return prefix + process.stdout.readline()
        return process.stdout.readline()
    return self._read_worker_response_line_with_timeout(
        process,
        redacted_command,
        deadline,
    )


def ShardLoomClient__read_worker_response_line_with_timeout(self, process, redacted_command, deadline):
    selector = _api.selectors.DefaultSelector()
    try:
        if process.stdout is None:
            return ""
        selector.register(process.stdout, _api.selectors.EVENT_READ)
        while True:
            buffered_line = self._pop_worker_stdout_buffered_line()
            if buffered_line is not None:
                return buffered_line
            remaining = deadline - _api.time.monotonic()
            if remaining <= 0:
                self._close_worker()
                raise _api.ShardLoomTimeoutError(redacted_command, self._timeout)
            events = selector.select(remaining)
            if not events:
                self._close_worker()
                raise _api.ShardLoomTimeoutError(redacted_command, self._timeout)
            chunk = _api.os.read(process.stdout.fileno(), 8192)
            if not chunk:
                if self._worker_stdout_buffer:
                    line = self._decode_worker_stdout(self._worker_stdout_buffer)
                    self._worker_stdout_buffer = b""
                    return line
                return ""
            self._worker_stdout_buffer += chunk
    finally:
        selector.close()


def ShardLoomClient__command_args(self, args):
    command_args = [str(arg) for arg in args]
    if any(arg == "--float-policy" or arg.startswith("--float-policy=") for arg in command_args):
        raise ValueError("--float-policy is fixed by the ShardLoomClient configuration")
    if any(arg == "--recursion-limit" or arg.startswith("--recursion-limit=") for arg in command_args):
        raise ValueError("--recursion-limit is fixed by the ShardLoomClient configuration")
    if self.float_policy != "strict":
        command_args.extend(("--float-policy", self.float_policy))
    if self.recursion_limit != 1000:
        command_args.extend(("--recursion-limit", str(self.recursion_limit)))
    self._append_json_format(command_args)
    return command_args


def ShardLoomClient__worker_enabled(self):
    if self._worker_disabled:
        return False
    if self._timeout is not None and _api.os.name == "nt":
        return False
    configured = self._effective_env().get(_api.ENV_PERSISTENT_WORKER)
    if configured is not None:
        return _api._env_truthy(configured)
    if self._use_persistent_worker is not None:
        return self._use_persistent_worker
    return True


def ShardLoomClient__ensure_worker(self):
    if self._worker_process is not None and self._worker_process.poll() is None:
        return self._worker_process
    command = self._binary_parts()
    command.append("python-worker")
    try:
        self._worker_process = _api.subprocess.Popen(
            command,
            cwd=self._cwd,
            env=self._effective_env(),
            text=True,
            stdin=_api.subprocess.PIPE,
            stdout=_api.subprocess.PIPE,
            stderr=_api.subprocess.DEVNULL,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise _api.ShardLoomBinaryNotFoundError(
            "ShardLoom CLI binary was not found while starting the "
            "persistent Python worker. Install the ShardLoom CLI package, "
            "put `shardloom` on PATH, or set SHARDLOOM_BIN to a valid binary."
        ) from exc
    except OSError as exc:
        raise _api._ShardLoomWorkerStartupError(
            f"ShardLoom persistent worker could not be started: {exc}"
        ) from exc
    self._worker_stdout_buffer = b""
    return self._worker_process


def ShardLoomClient__close_worker(self):
    process = self._worker_process
    self._worker_process = None
    self._worker_stdout_buffer = b""
    if process is None:
        return
    if process.stdin is not None:
        try:
            process.stdin.close()
        except OSError:
            pass
    if process.poll() is None:
        try:
            process.wait(timeout=0.2)
        except _api.subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=0.5)
            except _api.subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=0.5)
    for pipe in (process.stdout, process.stderr):
        if pipe is not None:
            try:
                pipe.close()
            except OSError:
                pass


def ShardLoomClient__resolved_binary(self):
    if self._binary is not None:
        return self._binary
    if self._resolved_binary_cache is not None:
        return self._resolved_binary_cache

    effective_env = self._effective_env()
    env_binary = effective_env.get(_api.ENV_BINARY)
    if env_binary:
        self._resolved_binary_cache = self._resolve_configured_binary(
            env_binary, effective_env
        )
        return self._resolved_binary_cache

    if self._repo_root is not None:
        candidate = self._repo_binary_candidate()
        if candidate is not None:
            self._resolved_binary_cache = candidate
            return self._resolved_binary_cache

    bundled_binary = self._bundled_binary_candidate()
    if bundled_binary is not None:
        self._resolved_binary_cache = bundled_binary
        return self._resolved_binary_cache

    path_binary = _api.shutil.which("shardloom", path=effective_env.get("PATH"))
    if path_binary is not None:
        self._resolved_binary_cache = path_binary
        return self._resolved_binary_cache

    raise _api.ShardLoomBinaryNotFoundError(
        "ShardLoom CLI binary could not be resolved. Install a supported "
        "ShardLoom wheel with a bundled CLI, install the ShardLoom CLI "
        "package, put `shardloom` on PATH, set SHARDLOOM_BIN to a valid "
        "binary, or set SHARDLOOM_REPO_ROOT to a checkout with "
        "target/release or target/debug binaries."
    )


def ShardLoomClient__resolve_configured_binary(self, value, env):
    configured = value.strip()
    if configured == "":
        raise _api.ShardLoomBinaryNotFoundError(f"{_api.ENV_BINARY} must not be empty")
    if _api._looks_like_path(configured):
        candidate = _api.Path(configured).expanduser()
        if not candidate.is_absolute():
            candidate = (self._cwd or _api.Path.cwd()) / candidate
        if candidate.is_file():
            return str(candidate)
        raise _api.ShardLoomBinaryNotFoundError(
            f"{_api.ENV_BINARY} points to {configured!r}, but that file does "
            "not exist. Set SHARDLOOM_BIN to the compiled ShardLoom CLI "
            "binary or remove it to use PATH discovery."
        )
    resolved = _api.shutil.which(configured, path=env.get("PATH"))
    if resolved is not None:
        return resolved
    raise _api.ShardLoomBinaryNotFoundError(
        f"{_api.ENV_BINARY}={configured!r} was not found on PATH. Install the "
        "ShardLoom CLI package, put the binary on PATH, or set "
        "SHARDLOOM_BIN to an absolute binary path."
    )


def ShardLoomClient__bundled_binary_candidate(self):
    executable = "shardloom.exe" if _api.os.name == "nt" else "shardloom"
    for tag in _api._bundled_cli_platform_tags():
        resource = _api.importlib_resources.files("shardloom").joinpath(
            _api.BUNDLED_CLI_RESOURCE_DIR,
            tag,
            executable,
        )
        try:
            if not resource.is_file():
                continue
            with _api.importlib_resources.as_file(resource) as candidate:
                candidate_path = _api.Path(candidate)
                if _api._is_executable_file(candidate_path):
                    return str(candidate_path)
        except (FileNotFoundError, ModuleNotFoundError, OSError):
            continue
    return None


def ShardLoomClient__append_json_format(command):
    if "--format" not in command:
        command.extend(["--format", "json"])
        return
    index = command.index("--format")
    try:
        value = command[index + 1]
    except IndexError as exc:
        raise ValueError("--format requires a value") from exc
    if value != "json":
        raise ValueError("ShardLoom Python client requires --format json")


def ShardLoomClient__parse_stdout(stdout, command):
    first_line = stdout.splitlines()[0] if stdout else ""
    if not first_line:
        raise _api.ShardLoomProtocolError(
            f"ShardLoom command emitted no JSON output: {' '.join(command)}"
        )
    try:
        payload = _api.json.loads(first_line)
    except _api.json.JSONDecodeError as exc:
        raise _api.ShardLoomProtocolError(
            f"ShardLoom command emitted invalid JSON: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise _api.ShardLoomProtocolError("ShardLoom JSON output envelope must be an object")
    try:
        return _api.OutputEnvelope.from_json(payload)
    except ValueError as exc:
        raise _api.ShardLoomProtocolError(str(exc)) from exc


def _bundled_cli_platform_tags():
    system = _api._normalized_bundled_cli_system_tag()
    arch = _api._normalized_bundled_cli_arch_tag()
    primary = f"{system}-{arch}"
    aliases: list[str] = [primary]
    if system == "macos" and arch == "aarch64":
        aliases.append("macos-arm64")
    if system == "windows":
        aliases.append(f"win-{arch}")
    return tuple(dict.fromkeys(aliases))
