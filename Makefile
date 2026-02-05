# Makefile: helper targets

.PHONY: log

# Usage:
#   make log FILE=convo.txt        # append convo.txt to project_journal.md
#   cat convo.txt | make log       # pipe via stdin

log:
	@python scripts/save_conversation.py $(FILE)
