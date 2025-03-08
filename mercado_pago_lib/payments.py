from mercado_pago_lib.api import MercadoPagoAPI
import pandas as pd
import json
from datetime import datetime, timedelta

class MercadoPagoPayment(MercadoPagoAPI):
    def __init__(self, environment='productive'):
        super().__init__(environment=environment)
    
    # CRUD operations
    def create_payment(self, payment_data):
        """
        Create a payment in Mercado Pago.
        
        :param payment_data: Dictionary with payment information
        :return: Payment response or error message
        """
        try:
            response = self.sdk.payment().create(payment_data)
            if response["status"] in [200, 201]:
                return response["response"]
            else:
                return f"Error creating payment: {response['response']}"
        except Exception as e:
            return f"Exception when creating payment: {str(e)}"
    
    def read_payment(self, payment_id):
        """
        Get payment details by ID.
        
        :param payment_id: The ID of the payment
        :return: Payment details or error message
        """
        try:
            response = self.sdk.payment().get(payment_id)
            if response["status"] == 200:
                return response["response"]
            else:
                return f"Error getting payment {payment_id}: {response['response']}"
        except Exception as e:
            return f"Exception when getting payment {payment_id}: {str(e)}"
    
    def update_payment(self, payment_id, payment_data):
        """
        Update a payment in Mercado Pago.
        
        :param payment_id: The ID of the payment to update
        :param payment_data: Dictionary with updated payment information
        :return: Updated payment response or error message
        """
        try:
            response = self.sdk.payment().update(payment_id, payment_data)
            if response["status"] == 200:
                return response["response"]
            else:
                return f"Error updating payment {payment_id}: {response['response']}"
        except Exception as e:
            return f"Exception when updating payment {payment_id}: {str(e)}"
    
    def read_payments(self, filters=None):
        """
        Search for payments with optional filters.
        
        :param filters: Dictionary with search filters
        :return: List of payments or error message
        """
        if filters is None:
            filters = {}
            
        try:
            response = self.sdk.payment().search(filters)
            if response["status"] == 200:
                return response["response"]
            else:
                return f"Error searching payments: {response['response']}"
        except Exception as e:
            return f"Exception when searching payments: {str(e)}"
    
    def read_payments_as_dataframe(self, days=30):
        """
        Get payments from the last specified days as a pandas DataFrame.
        
        :param days: Number of days to look back
        :return: DataFrame with payment data
        """
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Format dates for Mercado Pago API
            date_from = start_date.strftime("%Y-%m-%dT00:00:00.000-00:00")
            date_to = end_date.strftime("%Y-%m-%dT23:59:59.999-00:00")
            
            # Set up search filters
            filters = {
                "begin_date": date_from,
                "end_date": date_to,
                "limit": 100  # Adjust as needed
            }
            
            # Get payments
            payments = self.read_payments(filters)
            
            if isinstance(payments, str):  # Error message
                print(payments)
                return pd.DataFrame()
            
            # Convert to DataFrame
            if "results" in payments:
                df = pd.DataFrame(payments["results"])
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error getting payments as DataFrame: {str(e)}")
            return pd.DataFrame()
    
    def create_preference(self, preference_data):
        """
        Create a preference for a checkout.
        
        :param preference_data: Dictionary with preference information
        :return: Preference response or error message
        """
        try:
            response = self.sdk.preference().create(preference_data)
            if response["status"] in [200, 201]:
                return response["response"]
            else:
                return f"Error creating preference: {response['response']}"
        except Exception as e:
            return f"Exception when creating preference: {str(e)}"
    
    def read_preference(self, preference_id):
        """
        Get preference details by ID.
        
        :param preference_id: The ID of the preference
        :return: Preference details or error message
        """
        try:
            response = self.sdk.preference().get(preference_id)
            if response["status"] == 200:
                return response["response"]
            else:
                return f"Error getting preference {preference_id}: {response['response']}"
        except Exception as e:
            return f"Exception when getting preference {preference_id}: {str(e)}"
    
    def create_refund(self, payment_id, amount=None):
        """
        Refund a payment, either partially or totally.
        
        :param payment_id: The ID of the payment to refund
        :param amount: Amount to refund (if None, total refund)
        :return: Refund response or error message
        """
        try:
            if amount is None:
                # Total refund
                response = self.sdk.refund().create(payment_id)
            else:
                # Partial refund
                response = self.sdk.refund().create(payment_id, {"amount": amount})
                
            if response["status"] in [200, 201]:
                return response["response"]
            else:
                return f"Error refunding payment {payment_id}: {response['response']}"
        except Exception as e:
            return f"Exception when refunding payment {payment_id}: {str(e)}"
