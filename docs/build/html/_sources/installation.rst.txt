Installation
============

This guide covers setting up AmpyFin for local development or production.

Prerequisites
-------------
- Python 3.9+ (recommended)
- A virtual environment (optional, but recommended)

Using Requirements.txt
----------------------
You can install dependencies by running:

.. code-block:: bash

   pip install -r requirements.txt

Using Setup.py
--------------
Alternatively, install via setup.py:

.. code-block:: bash

   python setup.py install

Additional Tools
----------------
- Pre-commit hooks: see the file ``.pre-commit-config.yaml`` for code style checks.
- isort: automatically sort imports based on rules in ``.isort.cfg``.
- GitHub Actions: ``.github/workflows`` for CI/CD configuration.
- Weights & Biases (W&B): This is optional but recommended for experiment tracking. Use
  ``pip install wandb`` if required.
