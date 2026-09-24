# Faculty Systems Training

September 23, 2026

## Topics

1. Code as documents

  Tools: Notepad, TextEdit, email attachments, Dropbox.

  Code is a text file you write, save, and pass around. It's simple and needs no setup. The costs are that nothing checks your work, versions multiply ("analysis_final_v3_REAL.R"), and collaborators overwrite each other.

  Q: How do you know which copy is current, and could you rerun last year's analysis today?

2. Integrated environments

  Tools: RStudio, VS Code, Jupyter, PyCharm.

  The editor, console, debugger, file browser, and plots live in one place. You get syntax highlighting, autocomplete, linting, and immediate feedback. Notebooks belong here too, with their known tradeoff: they're great for exploration, but hidden state and out-of-order execution hurt reproducibility.

  Q: What does your environment catch that you'd otherwise miss?

3. Code as a tracked, reproducible project

  Tools: Git/GitHub, virtual environments (renv, venv, uv, conda), sometimes containers.

  This is a workflow paradigm rather than a tool. The project records its own history, its dependencies are declared, and anyone can clone it and run it. For faculty, this is the one that matters most for research reproducibility and for what students will face in industry. It works with any editor from paradigm 2 onward.

  Q: If a student or reviewer asked to rerun your results, what would break?

4. AI-assisted coding

  Tools: GitHub Copilot, Cursor, chat assistants used alongside the editor.

  AI suggests completions, explains errors, and writes functions on request, but the human drives line by line. The main questions are verification and skill-building. It's fast for people who can judge the output, and risky for people who can't.

  Q: How do you check AI-written code, and what does this mean for how students learn?

5. Agentic development

  Tools: Claude Code, Cursor's agent mode, similar tools.

  You describe a task, and the agent reads the codebase, edits multiple files, runs tests, and iterates. Your role shifts from writing code to specifying, reviewing, and steering. This builds on paradigm 3: agents work far better, and more safely, in a version-controlled project where changes can be inspected and rolled back.

  Q: What does it mean to be responsible for code you didn't write line by line?

## Resources

- [**Slides**](resources/IDEs-Coding-Environments.pdf)

