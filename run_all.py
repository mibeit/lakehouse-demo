from src.etl.sales.order_transformer import OrderTransformer
from src.etl.sales.orderline_transformer import OrderLineTransformer
from src.etl.sales.customer_transformer import CustomerTransformer
from src.etl.sales.invoice_transformer import InvoiceTransformer
from src.etl.sales.invoice_line_transformer import InvoiceLineTransformer

from src.etl.purchasing.purchase_order_transformer import PurchaseOrderTransformer
from src.etl.purchasing.purchase_order_line_transformer import PurchaseOrderLineTransformer
from src.etl.purchasing.supplier_transformer import SupplierTransformer
from src.etl.purchasing.supplier_transaction_transformer import SupplierTransactionTransformer

from src.etl.dimensions.dimension_transformer import DimensionTransformer, DIMENSIONS_CONFIG
from src.etl.dimensions.cities_transformer import CitiesTransformer
from src.etl.dimensions.province_transformer import ProvinceTransformer
from src.etl.dimensions.people_transformer import PeopleTransformer
from src.etl.dimensions.stock_item_transformer import StockItemTransformer
from src.etl.dimensions.stock_item_holdings_transformer import StockItemHoldingsTransformer

from src.etl.gold.dim_customer_transformer import DimCustomerTransformer
from src.etl.gold.dim_geography_transformer import DimGeographyTransformer
from src.etl.gold.dim_people_transformer import DimPeopleTransformer
from src.etl.gold.dim_stockitem_transformer import DimStockItemTransformer
from src.etl.gold.dim_supplier_transformer import DimSupplierTransformer
from src.etl.base_transformer import LOG_DIR

import logging
import time

# ------------------------------------------------------------------ #
#  Consolidated run_all logger                                       #
# ------------------------------------------------------------------ #

def _get_run_all_logger() -> logging.Logger:
    """Logger that writes every transformer's output into one file."""
    logger = logging.getLogger("run_all")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")

    fh = logging.FileHandler(LOG_DIR / "run_all.log", mode="w")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger


logger = _get_run_all_logger()


def _run_transformers(transformers: list, label: str) -> list[str]:
    """Run a list of transformers, collect errors, continue on failure.

    Returns:
        List of class names that failed.
    """
    failed: list[str] = []
    for transformer in transformers:
        name = transformer.__class__.__name__
        try:
            transformer.run()
            logger.info(f"[{label.upper()}] {name} -> OK")
        except Exception as e:
            failed.append(name)
            logger.error(f"[{label.upper()}] {name} -> FAILED: {e}")

    return failed


def run_all():
    t0 = time.perf_counter()
    logger.info("=" * 60)
    logger.info("Starting run_all pipeline")
    logger.info("=" * 60)

    # -- Bronze -> Silver ----------------------------------------------
    silver_transformers = [
        OrderTransformer(),
        OrderLineTransformer(),
        CustomerTransformer(),
        InvoiceTransformer(),
        InvoiceLineTransformer(),

        PurchaseOrderTransformer(),
        PurchaseOrderLineTransformer(),
        SupplierTransformer(),
        SupplierTransactionTransformer(),

        *[DimensionTransformer(name) for name in DIMENSIONS_CONFIG],

        CitiesTransformer(),
        ProvinceTransformer(),
        PeopleTransformer(),
        StockItemTransformer(),
        StockItemHoldingsTransformer(),
    ]

    # -- Silver -> Gold ------------------------------------------------
    gold_transformers = [
        DimCustomerTransformer(),
        DimGeographyTransformer(),
        DimPeopleTransformer(),
        DimStockItemTransformer(),
        DimSupplierTransformer(),
    ]
    silver_failed = _run_transformers(silver_transformers, "Silver")
    logger.info(f"Silver: {len(silver_transformers) - len(silver_failed)}/{len(silver_transformers)} OK")

    gold_failed = _run_transformers(gold_transformers, "Gold")
    logger.info(f"Gold:   {len(gold_transformers) - len(gold_failed)}/{len(gold_transformers)} OK")

    # -- Summary -------------------------------------------------------
    elapsed = time.perf_counter() - t0
    total = len(silver_transformers) + len(gold_transformers)
    all_failed = silver_failed + gold_failed

    logger.info("=" * 60)
    if all_failed:
        logger.warning(f"{len(all_failed)} transformer(s) failed: {', '.join(all_failed)}")
        logger.info(f"Total: {total - len(all_failed)}/{total} OK ({elapsed:.2f}s)")
    else:
        logger.info(f"All {total} Transformer successfully completed! ({elapsed:.2f}s)")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_all()
