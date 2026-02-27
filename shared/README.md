## This package is for modules shared between containers
### `models.py` includes tables for postgres database as models.
- If you want to add new tables, create new model in `models.py`
  - After this you can import your models in anywhere you need them with `from shared.models import yourModel`
- `core/app.py` should create all tables automatically when it starts up so no need for `db.create_all()`