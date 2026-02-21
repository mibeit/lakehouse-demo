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

import time


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
        except Exception as e:
            failed.append(name)
            print(f"\n[ERROR] {name} failed: {e}")

    return failed


def run_all():
    t0 = time.perf_counter()

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
    ]

    silver_failed = _run_transformers(silver_transformers, "Silver")
    print(f"\n{'=' * 60}")
    print(f"Silver: {len(silver_transformers) - len(silver_failed)}/{len(silver_transformers)} OK")

    gold_failed = _run_transformers(gold_transformers, "Gold")
    print(f"Gold:   {len(gold_transformers) - len(gold_failed)}/{len(gold_transformers)} OK")

    # -- Summary -------------------------------------------------------
    elapsed = time.perf_counter() - t0
    total = len(silver_transformers) + len(gold_transformers)
    all_failed = silver_failed + gold_failed

    print(f"{'=' * 60}")
    if all_failed:
        print(f"\n[WARN] {len(all_failed)} transformer(s) failed: {', '.join(all_failed)}")
        print(f"Total: {total - len(all_failed)}/{total} OK ({elapsed:.2f}s)")
    else:
        print(f"\nAll {total} Transformer successfully completed! ({elapsed:.2f}s)")


if __name__ == "__main__":
    run_all()
