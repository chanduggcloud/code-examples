package com.training.demo.schema.inference;

public class SalesRecord {

    public final Long id;

    public final String dateTime;
    public final String product;
    public final float price;
    public final String paymentType;
    public final String country;

    public SalesRecord(String dateTime, String product, float price,
                       String paymentType, String country) {
        this.id = Math.round(Math.random());

        this.dateTime = dateTime;
        this.product = product;
        this.price = price;
        this.paymentType = paymentType;
        this.country = country;
    }

    @Override
    public String toString() {
        return this.dateTime + ", " +
               this.product + ", " +
               this.price + ", " +
               this.paymentType + ", " +
               this.country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SalesRecord record = (SalesRecord) o;

        return dateTime.equals(record.dateTime) &&
                product.equals(record.product) &&
                price == record.price &&
                paymentType.equals(record.paymentType) &&
                country.equals(record.country);
    }

}
