# loom

**LLM execution runtime — parallel plan execution via streaming notation and composable IO primitives.**

LLMs generate execution plans. Loom runs them in parallel.

```
Current tool calling:    LLM → A → LLM → B → LLM → C → answer   (3 LLM calls, serial)
Loom:                    LLM → [A ∥ B ∥ C → answer]              (1 LLM call, parallel)
```

→ [Design Document](./DESIGN.md)

## Status

Early design phase.
