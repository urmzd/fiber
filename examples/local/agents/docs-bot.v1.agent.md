---
apiVersion: saige/v1
name: docs-bot
version: 1.0.0
description: answers questions about how we ship
model:
  # Change this to anthropic/<model id> and every other line of the example
  # stays the same.
  use: offline/scripted
---
You answer questions about the deploy pipeline. Cite your sources.
