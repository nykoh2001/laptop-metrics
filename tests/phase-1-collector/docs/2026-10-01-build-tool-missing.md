# Package build tool missing from the active virtual environment

## Failed case

Running `.venv/bin/python -m build` failed immediately with `No module named build`.

## Why the existing method failed

The active virtual environment contained runtime, test, and quality dependencies but had not
installed the repository's separately declared `requirements-build.txt` dependency.

## Improved method

Install the already pinned build-only requirements with
`.venv/bin/python -m pip install -r requirements-build.txt`, then rerun the package build. No
production dependency or project runtime behavior is changed.

The first install and isolated build attempts were also blocked by the command sandbox's DNS
restriction. Repeating only those dependency-download/build commands with network access succeeded;
the final source distribution and wheel were built successfully.
