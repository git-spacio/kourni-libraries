from typing import Dict, Optional, Union
from datetime import datetime
from .api import FlowAPI


class FlowPayment(FlowAPI):
    def __init__(self, sandbox: bool = False):
        super().__init__(sandbox)

    # CREATE methods
    def create_payment(self, 
                      commerce_order: str,
                      subject: str,
                      amount: float,
                      email: str,
                      url_confirmation: str,
                      url_return: str,
                      currency: str = 'CLP',
                      payment_method: Optional[int] = None,
                      optional: Optional[Dict] = None,
                      timeout: Optional[int] = None,
                      merchant_id: Optional[str] = None,
                      payment_currency: Optional[str] = None) -> Dict:
        """
        Create a payment order in Flow
        """
        params = {
            'commerceOrder': commerce_order,
            'subject': subject,
            'currency': currency,
            'amount': amount,
            'email': email,
            'urlConfirmation': url_confirmation,
            'urlReturn': url_return
        }

        # Add optional parameters if they exist
        if payment_method:
            params['paymentMethod'] = payment_method
        if optional:
            params['optional'] = optional
        if timeout:
            params['timeout'] = timeout
        if merchant_id:
            params['merchantId'] = merchant_id
        if payment_currency:
            params['payment_currency'] = payment_currency

        return self._make_request('payment/create', params, method='POST')

    def create_email_payment(self,
                           commerce_order: str,
                           subject: str,
                           amount: float,
                           email: str,
                           url_confirmation: str,
                           url_return: str,
                           currency: str = 'CLP',
                           forward_days_after: Optional[int] = None,
                           forward_times: Optional[int] = None,
                           optional: Optional[Dict] = None,
                           timeout: Optional[int] = None,
                           merchant_id: Optional[str] = None,
                           payment_currency: Optional[str] = None) -> Dict:
        """
        Create an email payment order in Flow
        """
        params = {
            'commerceOrder': commerce_order,
            'subject': subject,
            'currency': currency,
            'amount': amount,
            'email': email,
            'urlConfirmation': url_confirmation,
            'urlReturn': url_return
        }

        # Add optional parameters if they exist
        if forward_days_after:
            params['forward_days_after'] = forward_days_after
        if forward_times:
            params['forward_times'] = forward_times
        if optional:
            params['optional'] = optional
        if timeout:
            params['timeout'] = timeout
        if merchant_id:
            params['merchantId'] = merchant_id
        if payment_currency:
            params['payment_currency'] = payment_currency

        return self._make_request('payment/createEmail', params, method='POST')

    # READ methods
    def read_payment_status(self, token: str) -> Dict:
        """
        Get payment status using token
        """
        params = {'token': token}
        return self._make_request('payment/getStatus', params)

    def read_payment_status_extended(self, token: str) -> Dict:
        """
        Get extended payment status using token
        """
        params = {'token': token}
        return self._make_request('payment/getStatusExtended', params)

    def read_payment_by_commerce_id(self, commerce_id: str) -> Dict:
        """
        Get payment status using commerce ID
        """
        params = {'commerceId': commerce_id}
        return self._make_request('payment/getStatusByCommerceId', params)

    def read_payment_by_flow_order(self, flow_order: Union[int, str]) -> Dict:
        """
        Get payment status using Flow order number
        """
        params = {'flowOrder': flow_order}
        return self._make_request('payment/getStatusByFlowOrder', params)

    def read_payment_by_flow_order_extended(self, flow_order: Union[int, str]) -> Dict:
        """
        Get extended payment status using Flow order number
        """
        params = {'flowOrder': flow_order}
        return self._make_request('payment/getStatusByFlowOrderExtended', params)

    def read_payments_by_date(self, 
                            date: Union[str, datetime],
                            start: Optional[int] = None,
                            limit: Optional[int] = 100) -> Dict:
        """
        Get list of payments for a specific date
        """
        # Convert datetime to string if needed
        if isinstance(date, datetime):
            date = date.strftime('%Y-%m-%d')

        params = {'date': date}
        if start is not None:
            params['start'] = start
        if limit is not None:
            params['limit'] = limit

        return self._make_request('payment/getPayments', params)

    def read_transactions_by_date(self,
                                date: Union[str, datetime],
                                start: Optional[int] = None,
                                limit: Optional[int] = None) -> Dict:
        """
        Get list of transactions for a specific date
        """
        # Convert datetime to string if needed
        if isinstance(date, datetime):
            date = date.strftime('%Y-%m-%d')

        params = {'date': date}
        if start is not None:
            params['start'] = start
        if limit is not None:
            params['limit'] = limit

        return self._make_request('payment/getTransactions', params)
