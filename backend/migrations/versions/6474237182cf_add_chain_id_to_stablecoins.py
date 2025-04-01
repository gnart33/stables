"""Add chain_id to stablecoins

Revision ID: 6474237182cf
Revises: 85499c78df40
Create Date: 2025-04-01 18:27:33.403914

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "6474237182cf"
down_revision: Union[str, None] = "85499c78df40"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    # First add columns as nullable
    op.add_column(
        "aggregated_metrics",
        sa.Column("total_transfer_volume", sa.Float(), nullable=True),
    )
    op.add_column(
        "aggregated_metrics", sa.Column("transfer_count", sa.Integer(), nullable=True)
    )
    op.add_column(
        "stablecoins", sa.Column("contract_address", sa.String(), nullable=True)
    )
    op.add_column("stablecoins", sa.Column("chain_id", sa.Integer(), nullable=True))

    # Create foreign key
    op.create_foreign_key(None, "stablecoins", "chains", ["chain_id"], ["id"])

    # Update existing data
    op.execute(
        """
        UPDATE stablecoins 
        SET contract_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
            chain_id = (SELECT id FROM chains WHERE name = 'ethereum' LIMIT 1)
        WHERE contract_address IS NULL
    """
    )

    op.execute(
        """
        UPDATE aggregated_metrics 
        SET total_transfer_volume = 0,
            transfer_count = 0
        WHERE total_transfer_volume IS NULL
    """
    )

    # Make columns non-nullable
    op.alter_column(
        "aggregated_metrics",
        "total_transfer_volume",
        existing_type=sa.Float(),
        nullable=False,
    )
    op.alter_column(
        "aggregated_metrics",
        "transfer_count",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.alter_column(
        "stablecoins", "contract_address", existing_type=sa.String(), nullable=False
    )
    op.alter_column(
        "stablecoins", "chain_id", existing_type=sa.Integer(), nullable=False
    )

    # Clean up old columns
    op.alter_column(
        "aggregated_metrics",
        "stablecoin_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "aggregated_metrics",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
    )
    op.drop_index("ix_aggregated_metrics_id", table_name="aggregated_metrics")
    op.drop_index("ix_aggregated_metrics_timestamp", table_name="aggregated_metrics")
    op.drop_constraint(
        "aggregated_metrics_chain_id_fkey", "aggregated_metrics", type_="foreignkey"
    )
    op.drop_column("aggregated_metrics", "transferred")
    op.drop_column("aggregated_metrics", "burned")
    op.drop_column("aggregated_metrics", "minted")
    op.drop_column("aggregated_metrics", "chain_id")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, "stablecoins", type_="foreignkey")
    op.drop_column("stablecoins", "chain_id")
    op.drop_column("stablecoins", "contract_address")
    op.add_column(
        "aggregated_metrics",
        sa.Column("chain_id", sa.INTEGER(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "aggregated_metrics",
        sa.Column(
            "minted",
            sa.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "aggregated_metrics",
        sa.Column(
            "burned",
            sa.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "aggregated_metrics",
        sa.Column(
            "transferred",
            sa.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.create_foreign_key(
        "aggregated_metrics_chain_id_fkey",
        "aggregated_metrics",
        "chains",
        ["chain_id"],
        ["id"],
    )
    op.create_index(
        "ix_aggregated_metrics_timestamp",
        "aggregated_metrics",
        ["timestamp"],
        unique=False,
    )
    op.create_index(
        "ix_aggregated_metrics_id", "aggregated_metrics", ["id"], unique=False
    )
    op.alter_column(
        "aggregated_metrics",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
    )
    op.alter_column(
        "aggregated_metrics", "stablecoin_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.drop_column("aggregated_metrics", "transfer_count")
    op.drop_column("aggregated_metrics", "total_transfer_volume")
    # ### end Alembic commands ###
