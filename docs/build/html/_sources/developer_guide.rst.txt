Developer Guide
===============

Directory Layout
----------------
.. code-block:: text

   ├─ .github/
   │   └─ workflows/             # CI/CD definitions
   ├─ TradeSim/
   │   ├─ main.py                # Main entry for training/testing
   │   ├─ push.py
   │   ├─ training.py
   │   ├─ testing.py
   │   ├─ utils.py
   │   └─ variables.py
   ├─ helper_files/
   │   └─ client_helper.py
   ├─ strategies/
   │   ├─ archived_strategies/
   │   ├─ mean_reversion.py
   │   ├─ momentum.py
   │   ├─ arbitrage.py
   │   └─ ai_custom.py
   ├─ control.py
   ├─ ranking_client.py
   ├─ trading_client.py
   ├─ wandb/
   └─ docs/
       ├─ index.rst
       └─ ...

GitHub Actions
--------------
Continuous integration is set up via workflows in ``.github/workflows``. This ensures:
- Linting and formatting checks (using black, isort, flake8, or pre-commit).
- Testing on multiple Python versions.
- Optionally deploy docs or PyPI package.

Pre-commit Hooks
----------------
We use `.pre-commit-config.yaml` to enforce a consistent code style and catch common errors.

.. code-block:: bash

   # To set up:
   pip install pre-commit
   pre-commit install
   # Now each commit triggers checks defined in .pre-commit-config.yaml

Testing
-------
We recommend writing tests to cover your strategies, clients, and helpers. Use either:
- **pytest**: A popular testing framework
- **unittest**: Built-in Python testing

Contributing
------------
Contributions are welcome! Please open a pull request or raise an issue. 
Remember to run pre-commit hooks and ensure your code style is consistent.
