Usage
=====

Basic Usage
-----------
To start using AmpyFin, run the main script in the `TradeSim` folder. For instance:

.. code-block:: bash

   python TradeSim/main.py --config my_config.json

Check out the available command-line arguments with:

.. code-block:: bash

   python TradeSim/main.py --help

Parameter Configuration
-----------------------
You can adjust trading parameters via ``control.py``. For example:

.. code-block:: python

   from control import TradingParams

   params = TradingParams(
       strategy="momentum",
       capital=10000,
       max_positions=5
   )
   # Then pass params to your trading client or main script

Key Scripts
-----------
- **training.py**: trains models or strategies
- **testing.py**: tests trained models on historical data
- **push.py**: pushes results or logs to a remote server
- **trading_client.py**: executes trades based on AI or rule-based strategies
- **ranking_client.py**: ranks and evaluates multiple strategies
