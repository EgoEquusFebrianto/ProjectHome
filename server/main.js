import express from "express";
import { PrismaClient } from "@prisma/client";
import { faker } from "@faker-js/faker";

const app = express();
const prisma = new PrismaClient();
const PORT = 4000;

// SSE endpoint
app.get("/stream/api/retail-transaction", async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  console.log("🟢 Client connected to /stream/api/retail-transaction");

  const interval = setInterval(async () => {
    try {
      const order = {
        order_id: faker.string.uuid(),
        product_id: faker.string.alphanumeric(8),
        product_name: faker.commerce.productName(),
        category: faker.commerce.department(),
        customer_id: faker.string.alphanumeric(6),
        customer_name: faker.person.fullName(),
        quantity: faker.number.int({ min: 1, max: 10 }),
        price_per_product: faker.number.int({ min: 10, max: 650 }),
        payment_method: faker.helpers.arrayElement(["cash", "credit_card", "ewallet"]),
        status: faker.helpers.arrayElement(["PENDING", "PAID", "SHIPPED"]),
        orderAt: new Date(),
      };

      await prisma.orders.create({ data: order });

      res.write(`data: ${JSON.stringify(order)}\n\n`);
      console.log(`Order sent & saved: ${order.order_id}`);

    } catch (error) {
      console.error("Error generating or saving order:", error);
      res.write(`event: error\ndata: ${JSON.stringify({ message: error.message })}\n\n`);
      res.flush();
    }
  }, 2000);

  req.on("close", async () => {
    console.log("🔴 Client disconnected.");
    clearInterval(interval);

    try {
      const deleteResult = await prisma.orders.deleteMany();
      console.log(`Deleted ${deleteResult.count} orders from database`);      
      
    } catch (error) {
      console.error("Error deleting session orders:", error);
    }
  });
});

app.listen(PORT, () => {
  console.log(`Streaming API running at http://localhost:${PORT}/stream/api/retail-transaction`);
});
