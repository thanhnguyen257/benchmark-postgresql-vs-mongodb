#!/bin/bash

set -e

echo "Initializing MongoDB..."

mongoimport \
  --username "$MONGO_USER" \
  --password "$MONGO_PASSWORD" \
  --authenticationDatabase admin \
  --db "$MONGO_DB" \
  --collection users \
  --file /dummy_data/users.json \
  --numInsertionWorkers 4

mongoimport \
  --username "$MONGO_USER" \
  --password "$MONGO_PASSWORD" \
  --authenticationDatabase admin \
  --db "$MONGO_DB" \
  --collection products \
  --file /dummy_data/products.json \
  --numInsertionWorkers 4

mongoimport \
  --username "$MONGO_USER" \
  --password "$MONGO_PASSWORD" \
  --authenticationDatabase admin \
  --db "$MONGO_DB" \
  --collection orders \
  --file /dummy_data/orders_mongo.json \
  --numInsertionWorkers 4

# mongosh --username "$MONGO_USER" \
#         --password "$MONGO_PASSWORD" \
#         --authenticationDatabase admin \
#         "$MONGO_DB" <<EOF

# db.users.createIndex({ userId: 1 }, { unique: true })

# db.products.createIndex({ productId: 1 }, { unique: true })
# db.products.createIndex({ stock: 1 })

# db.orders.createIndex({ orderId: 1 }, { unique: true })
# db.orders.createIndex({ orderDate: 1 })
# db.orders.createIndex({ "user.userId": 1 })

# EOF
echo "MongoDB initialization completed!"