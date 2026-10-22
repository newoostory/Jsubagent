# Jsubagent

Independent subagents for [Pi](https://github.com/earendil-works/pi). Delegate a bounded task, exchange messages, and wait for the result without polling the model.

Pi handles model sessions and authentication. Jsubagent adds durable task state, isolated child context, workspace preparation, messaging, and event-driven waits. The current release focuses on subagents. Swarm strategies and a shared project board are future work.

## Install

Requires a **Node-based Pi 1.3.0 installation**, **Node.js 22.19.0 or newer**, **Git**, and **ripgrep** (`rg` on your PATH). Development checks use Node 24.19.0 on macOS; CI also checks Linux and Node 22.19.0. Windows and standalone compiled Pi binaries have not been verified.

```sh
pi install git:github.com/newoostory/Jsubagent@v0.1.0
```

Start a new Pi session after installation. Use your existing Pi login and selected model. The default `main` profile inherits that model when a child is admitted. No separate account file is bundled.

If an older `pi-agent` or `pi-subagents` extension is already installed, replace its package entry before loading Jsubagent: both register the `agent` tool.

## Use

Ask Pi to delegate a task, or have it call `agent` with these inputs. Give children the goal, permitted paths, relevant input revision, constraints, acceptance checks, and a stop condition. Parent conversation is not passed automatically.

```json
{
  "action": "spawn",
  "tasks": [{
    "name": "inspect-parser",
    "role": "scout",
    "task": "Goal: explain how src/parser handles empty input. Owned paths: src/parser. Input: current checkout. Constraints: read only; no commands or edits. Acceptance: cite the relevant files and branches. Stop: once the behavior is established, or report the exact missing input."
  }]
}
```

Keep the returned `agentId`. To wait for selected agents:

```json
{"action":"wait","agentIds":["returned-agent-id"],"mode":"all"}
```

| Action | Purpose |
| --- | --- |
| `catalog` | Inspect configured roles and profiles on demand. |
| `spawn` | Admit 1–8 tasks per call, with optional dependencies and result handoffs. |
| `status` | Read a current snapshot. |
| `wait` | Wait for selected agents using `all` or `any`; unread questions, blockers, or actionable failures can wake it early. |
| `send` | Send a turn-boundary steer or follow-up to selected children. |
| `cancel` | Explicitly cancel selected tasks. |
| `resume` | Continue a recoverable task with a new instruction. |
| `archive` | Archive a task through its lifecycle rules. |
| `read` | Read bounded results, transcripts, messages, artifacts, or receipts. |

`wait` has no polling timer or time limit. `any` waits for the first selected agent to settle; use one ID to wait for a specific child. By default, unread important messages can wake either mode. Aborting the wait leaves its children running. A settled task may have failed: inspect its returned state and error.

Routine child progress is stored without starting a main-model turn. Important messages can steer an active main loop at its next model call. A completion wakes a main that is waiting for it; an idle completion notice waits for the next user-started turn.

`/hub` opens a read-only roster and bounded detail view in the Pi terminal UI. `/agent` lets you choose the optional coordinator restriction; direct work is the default.

## Roles and workspaces

| Built-in role | Workspace | Intended use |
| --- | --- | --- |
| `scout` | Read only | Find decision-relevant repository evidence. |
| `worker` | Isolated worktree or pinned directory copy | Implement a bounded task. |
| `reviewer` | Read only | Review a pinned result or revision. |
| `integrator` | Shared | Integrate explicitly assigned changes. |
| `verifier` | Shared | Run explicitly assigned checks. |

Each child has its own model context. Role and profile choices, input identity, and workspace are pinned for the task. Git workspaces start from the admitted clean snapshot; non-Git inputs use a pinned directory snapshot. Later dirty source edits are not implicitly imported. Worker publication and worktree preparation do not automatically merge changes into the destination.

Agents run through a bounded worker pool. Agent count and tool-process count have separate limits. Local metadata, checkpoints, messages, and artifacts survive host restoration. Recovery fences ambiguous effects instead of automatically replaying an uncertain command. Filesystem read/edit helpers are confined to the child workspace; **bash is a trusted shell, not an OS sandbox**. Children do not automatically inherit main-session hooks, skills, MCP connections, or approvals.

Stopping the main model turn leaves children running. A normal Pi quit closes the process-owned hosts and preserves checkpoints. Terminal closure and abrupt process death depend on OS signal delivery; there is no guarantee that arbitrary external tool processes survive or stop atomically. Keeping Pi running in tmux keeps the session running normally.

## Configuration

Copy the relevant fields from [config.example.json](config.example.json) into:

```text
~/.pi/agent/pi-agent/config.json
```

`PI_CODING_AGENT_DIR` changes the base agent directory. The existing `pi-agent` configuration and state namespace is retained for compatibility.

User configuration controls runtime limits, profiles, provider modules, and role files. A trusted project's `.pi/pi-agent.json` can supply profiles and roles. Role files use JSON frontmatter between `---` lines followed by the role prompt; see [agents/scout.md](agents/scout.md). The host validates role/profile pins and captures configuration for admitted tasks.

The ordinary subagent path has no dependency on the maintainer's private extensions. An optional `pi-web-fusion/runtime` adapter remains for existing installations: it is **not bundled**. Without it, ordinary children receive no web tools. A role that explicitly requires web tools fails with `CAPABILITY` when the adapter is unavailable; an installed but broken adapter also fails instead of choosing another provider. Legacy PLAN/team adapters are not part of this release's public `agent` actions.

## Development

```sh
git clone https://github.com/newoostory/Jsubagent.git
cd Jsubagent
npm ci --ignore-scripts
npm run check
npm test
npm run check:subagents
npm run check:flow
npm run check:pack
npm run check:install
```

These checks use temporary workspaces, local sockets, and a loopback SSE provider with a synthetic API key. They exercise the real Pi SDK without calling a live model or reading your installed account. The public-loop check verifies context isolation, the lifecycle tool surface, and absence of unsolicited follow-up requests. The runtime-flow check covers concurrent work, file tools, messaging, cancellation, and restart. The install check uses Pi's real extension-path loader and only packed runtime files, with no extension `node_modules`; native hosts and workers resolve their peers from the running Pi installation.

This is an early release. Functional fixture checks do not establish live-provider reliability, model quality, cache hit rate, latency, or memory improvements. Bug reports should include the Pi/Node versions and a small reproduction with private paths, prompts, and credentials removed.

## License

MIT. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
