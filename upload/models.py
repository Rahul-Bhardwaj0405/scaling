from django.db import models

# Create your models here.

class BookingData(models.Model):
    bank_code = models.IntegerField()  # Bank numeric code
    txn_date = models.DateField(null=True, blank=True)
    credited_on_date = models.DateField(null=True, blank=True)
    booking_amount = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    irctc_order_no = models.BigIntegerField(null=True, blank=True)
    bank_booking_ref_no = models.BigIntegerField(null=True, blank=True)



    # # New bank-specific fields
    # branch_code = models.CharField(max_length=10, null=True, blank=True)  # Example for IDBI
    # other_column = models.CharField(max_length=255, null=True, blank=True)  # Add as needed for other banks


    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['txn_date','bank_code', 'credited_on_date', 'irctc_order_no', 'bank_booking_ref_no'], name='unique_bookingdata_constraint')
        ]

class RefundData(models.Model):
    bank_code = models.IntegerField()  # Bank numeric code
    refund_date = models.DateField(null=True, blank=True)
    debited_on_date = models.DateField(null=True, blank=True)
    refund_amount = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    irctc_order_no = models.BigIntegerField(null=True, blank=True)
    bank_booking_ref_no = models.BigIntegerField(null=True, blank=True)
    bank_refund_ref_no = models.BigIntegerField(null=True, blank=True)

    
    # # New bank-specific fields
    # branch_code = models.CharField(max_length=10, null=True, blank=True)  # Example for IDBI
    # other_column = models.CharField(max_length=255, null=True, blank=True)  # Add as needed for other banks


    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['refund_date', 'bank_code', 'debited_on_date', 'irctc_order_no', 'bank_booking_ref_no', 'bank_refund_ref_no'], name='unique_refunddata_constraint')
        ]
