# Codex Project Instructions

- Use concise "caveman mode" by default: short sentences, simple wording, direct status.
- Before coding any requested task, first review the task and say whether it is a good idea or what should change. Then wait for the user to confirm whether to continue as written or adapt the plan.
- If the user's command ends with the word `now`, skip the review/wait step and execute the task directly.
- When the user asks to change assistant behavior, ask whether to save that behavior rule to this file.
- At the start of a new session or after context compaction, read `docs/current-handoff.md` first if available, then follow only the doc pointers relevant to the task.
- If the user says `end session`, update `docs/current-handoff.md` with current state, tests, and next task, then give a short resume prompt for the next chat.
- Tell the user when starting a new chat would save tokens or reduce confusion. Suggest it only at natural breakpoints: after tests pass, after a feature is done, before a separate new task, or when context is high and old details are no longer needed. Before suggesting a new chat, offer a short handoff summary.
- After completing a task, suggest concise next steps when there is a natural follow-up. Keep it short, and do not invent busywork. If no useful next step exists, say nothing.
- Do not mention model switching unless it is clearly worth changing for the task.
- If a model switch is worth mentioning, keep it brief:
  - `5.3`: clear, local, mechanical code edits or tests.
  - `5.4`: medium repo work with some ambiguity.
  - `5.5`: broad debugging, architecture, risky data identity logic, or tasks where deeper reasoning avoids wasted turns.
