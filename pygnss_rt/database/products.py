"""
Product database operations.

Replaces Perl PROD.pm module.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pygnss_rt.database.models import Product, ProductTier, ProductType
from pygnss_rt.utils.dates import GNSSDate

if TYPE_CHECKING:
    from pygnss_rt.database.connection import DatabaseManager


class ProductManager:
    """Manages GNSS product records in the database."""

    def __init__(self, db: "DatabaseManager"):
        """Initialize product manager.

        Args:
            db: Database manager instance
        """
        self.db = db

    def add_product(self, product: Product) -> int:
        """Add a product record.

        Args:
            product: Product to add

        Returns:
            ID of inserted record
        """
        result = self.db.execute(
            """
            INSERT INTO products (
                product_type, provider, tier, mjd, gps_week, day_of_week,
                filename, local_path, file_size, checksum
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (
                product.product_type.value,
                product.provider,
                product.tier.value,
                product.mjd,
                product.gps_week,
                product.day_of_week,
                product.filename,
                product.local_path,
                product.file_size,
                product.checksum,
            ),
        )
        row = result.fetchone()
        return row[0] if row else 0

    def get_product(
        self,
        product_type: ProductType,
        provider: str,
        tier: ProductTier,
        date: GNSSDate,
    ) -> Product | None:
        """Get a product by type, provider, tier, and date.

        Args:
            product_type: Type of product
            provider: Product provider
            tier: Product tier
            date: Date

        Returns:
            Product if found, None otherwise
        """
        row = self.db.fetchone(
            """
            SELECT id, product_type, provider, tier, mjd, gps_week, day_of_week,
                   filename, local_path, file_size, checksum, download_time
            FROM products
            WHERE product_type = ? AND provider = ? AND tier = ?
              AND mjd >= ? AND mjd < ?
            """,
            (
                product_type.value,
                provider,
                tier.value,
                date.mjd,
                date.mjd + 1,
            ),
        )

        if not row:
            return None

        return Product(
            id=row[0],
            product_type=ProductType(row[1]),
            provider=row[2],
            tier=ProductTier(row[3]),
            mjd=row[4],
            gps_week=row[5],
            day_of_week=row[6],
            filename=row[7],
            local_path=row[8],
            file_size=row[9],
            checksum=row[10],
            download_time=row[11],
        )

    def product_exists(
        self,
        product_type: ProductType,
        provider: str,
        tier: ProductTier,
        date: GNSSDate,
    ) -> bool:
        """Check if product exists in database.

        Args:
            product_type: Type of product
            provider: Product provider
            tier: Product tier
            date: Date

        Returns:
            True if product exists
        """
        row = self.db.fetchone(
            """
            SELECT 1 FROM products
            WHERE product_type = ? AND provider = ? AND tier = ?
              AND mjd >= ? AND mjd < ?
            LIMIT 1
            """,
            (
                product_type.value,
                provider,
                tier.value,
                date.mjd,
                date.mjd + 1,
            ),
        )
        return row is not None

    def get_local_path(
        self,
        product_type: ProductType,
        provider: str,
        tier: ProductTier,
        date: GNSSDate,
    ) -> Path | None:
        """Get local path of a downloaded product.

        Args:
            product_type: Type of product
            provider: Product provider
            tier: Product tier
            date: Date

        Returns:
            Local path if product downloaded, None otherwise
        """
        row = self.db.fetchone(
            """
            SELECT local_path FROM products
            WHERE product_type = ? AND provider = ? AND tier = ?
              AND mjd >= ? AND mjd < ?
              AND local_path IS NOT NULL
            """,
            (
                product_type.value,
                provider,
                tier.value,
                date.mjd,
                date.mjd + 1,
            ),
        )
        return Path(row[0]) if row else None

    def update_local_path(
        self,
        product_id: int,
        local_path: Path,
        file_size: int | None = None,
    ) -> None:
        """Update local path after download.

        Args:
            product_id: Product record ID
            local_path: Local file path
            file_size: File size in bytes
        """
        self.db.execute(
            """
            UPDATE products
            SET local_path = ?, file_size = ?
            WHERE id = ?
            """,
            (str(local_path), file_size, product_id),
        )

    def list_products(
        self,
        product_type: ProductType | None = None,
        provider: str | None = None,
        start_mjd: float | None = None,
        end_mjd: float | None = None,
    ) -> list[Product]:
        """List products with optional filters.

        Args:
            product_type: Filter by type
            provider: Filter by provider
            start_mjd: Start date (MJD)
            end_mjd: End date (MJD)

        Returns:
            List of matching products
        """
        query = """
            SELECT id, product_type, provider, tier, mjd, gps_week, day_of_week,
                   filename, local_path, file_size, checksum, download_time
            FROM products
            WHERE 1=1
        """
        params: list[str | float] = []

        if product_type:
            query += " AND product_type = ?"
            params.append(product_type.value)
        if provider:
            query += " AND provider = ?"
            params.append(provider)
        if start_mjd:
            query += " AND mjd >= ?"
            params.append(start_mjd)
        if end_mjd:
            query += " AND mjd <= ?"
            params.append(end_mjd)

        query += " ORDER BY mjd"

        rows = self.db.fetchall(query, tuple(params) if params else None)

        return [
            Product(
                id=row[0],
                product_type=ProductType(row[1]),
                provider=row[2],
                tier=ProductTier(row[3]),
                mjd=row[4],
                gps_week=row[5],
                day_of_week=row[6],
                filename=row[7],
                local_path=row[8],
                file_size=row[9],
                checksum=row[10],
                download_time=row[11],
            )
            for row in rows
        ]
