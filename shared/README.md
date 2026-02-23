## This package is for modules shared between containers
### `models.py` includes tables for postgres database as models.
- If you want to add new tables, create new model in `models.py`, also add `yourModel` into import list of `__init__.py`
  - After this you can import your models in anywhere you need them with `from shared import yourModel`
- `core/app.py` should create all tables automatically when it starts up