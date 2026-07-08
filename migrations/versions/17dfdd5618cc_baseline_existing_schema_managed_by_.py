"""baseline: existing schema managed by init_schema()

Intentionally empty. Tables/indexes up to this point were created by
``PostgresRepository.init_schema()`` (``create_all()`` + hand-written
blocking-index DDL), not by Alembic. This revision exists only as the
starting point every future migration builds on — any environment that
already has the schema (dev, prod) should be marked as being at this
revision via ``alembic stamp head`` rather than having this migration
actually run against it.

Revision ID: 17dfdd5618cc
Revises:
Create Date: 2026-07-08 11:30:38.664155

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '17dfdd5618cc'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
