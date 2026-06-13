-- CreateTable
CREATE TABLE "Orders" (
    "order_id" TEXT NOT NULL,
    "product_id" TEXT NOT NULL,
    "product_name" TEXT NOT NULL,
    "category" TEXT,
    "customer_id" TEXT,
    "customer_name" TEXT,
    "quantity" INTEGER NOT NULL,
    "price_per_product" INTEGER NOT NULL,
    "payment_method" TEXT,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "orderAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Orders_pkey" PRIMARY KEY ("order_id")
);
