#!/usr/bin/env bash
# Shared runtime JAR staging, validation and bounded shutdown for the launchers.

launcher_snapshot_jar() {
  local source_jar="$1" runtime_dir="$2" staged_jar runtime_jar
  mkdir -p "${runtime_dir}" || return 1
  staged_jar="$(mktemp "${runtime_dir}/.taskite-distribution.XXXXXX")" || return 1
  runtime_jar="${runtime_dir}/taskite-distribution.jar"
  # Maven recreates the build JAR in place before shading it. Keep the JVM on a separate inode;
  # replace the runtime copy atomically so any process already reading it keeps its old JAR.
  if ! cp "${source_jar}" "${staged_jar}" || ! mv -f "${staged_jar}" "${runtime_jar}"; then
    rm -f "${staged_jar}"
    return 1
  fi
  printf '%s\n' "${runtime_jar}"
}

launcher_validate_ports() {
  local ports=("$@") seen=() port previous
  for port in "${ports[@]}"; do
    if [[ ! "${port}" =~ ^[1-9][0-9]*$ ]] || ((port > 65535)); then
      echo "error: invalid port '${port}'; expected 1..65535" >&2
      return 2
    fi
    for previous in "${seen[@]:-}"; do
      if [[ "${previous}" == "${port}" ]]; then
        echo "error: duplicate listener port '${port}'" >&2
        return 2
      fi
    done
    seen+=("${port}")
  done
}

launcher_stop() {
  local pid_file="$1" label="$2" pid timeout="${TASKITE_LAUNCHER_STOP_TIMEOUT:-30s}"
  [[ -f "${pid_file}" ]] || return 0
  if [[ ! "${timeout}" =~ ^([1-9][0-9]*)(ms|s|m)$ ]]; then
    echo "error: invalid TASKITE_LAUNCHER_STOP_TIMEOUT '${timeout}'" >&2
    return 2
  fi
  local seconds="${BASH_REMATCH[1]}"
  case "${BASH_REMATCH[2]}" in
    ms) seconds=$(((seconds + 999) / 1000)) ;;
    m) seconds=$((seconds * 60)) ;;
  esac
  pid="$(cat "${pid_file}")"
  if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "stopping ${label} (pid ${pid})"
    kill "${pid}"
    local deadline=$((SECONDS + seconds))
    while kill -0 "${pid}" 2>/dev/null && ((SECONDS < deadline)); do
      sleep 0.2
    done
    if kill -0 "${pid}" 2>/dev/null; then
      echo "error: ${label} did not stop within ${timeout}" >&2
      return 1
    fi
  fi
  rm -f "${pid_file}"
}

launcher_check_pid() {
  local pid_file="$1" label="$2" pid
  [[ -f "${pid_file}" ]] || return 0
  pid="$(cat "${pid_file}")"
  if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "error: ${label} is already running (pid ${pid})" >&2
    return 1
  fi
}
