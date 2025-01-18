import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/')
from shopify_lib.api import ShopifyAPI
from bs4 import BeautifulSoup

class ShopifyPolicies(ShopifyAPI):
    def __init__(self, shop_url=None, api_password=None, api_version="2024-01"):
        super().__init__(shop_url, api_password, api_version)

    def _parse_policy_content(self, html_content):
        """
        Parse HTML content and extract clean text.
        
        Args:
            html_content (str): HTML content of the policy
            
        Returns:
            str: Clean text content without HTML tags
        """
        if not html_content:
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for element in soup(['script', 'style']):
            element.decompose()
            
        # Get text and clean up whitespace
        text = soup.get_text(separator='\n')
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return '\n'.join(lines)

    def read_refund_policy(self):
        """
        Retrieve and parse the refund policy.
        
        Returns:
            dict: Refund policy title and clean text content
        """
        policies = self.read('policies.json')['policies']
        refund_policy = next((policy for policy in policies if policy['handle'] == 'refund-policy'), None)
        
        if not refund_policy:
            return {'title': None, 'body': None}
            
        return {
            'title': refund_policy.get('title'),
            'body': self._parse_policy_content(refund_policy.get('body'))
        }

    def read_privacy_policy(self):
        """
        Retrieve and parse the privacy policy.
        
        Returns:
            dict: Privacy policy title and clean text content
        """
        policies = self.read('policies.json')['policies']
        privacy_policy = next((policy for policy in policies if policy['handle'] == 'privacy-policy'), None)
        
        if not privacy_policy:
            return {'title': None, 'body': None}
            
        return {
            'title': privacy_policy.get('title'),
            'body': self._parse_policy_content(privacy_policy.get('body'))
        }

    def read_terms_of_service(self):
        """
        Retrieve and parse the terms of service.
        
        Returns:
            dict: Terms of service title and clean text content
        """
        policies = self.read('policies.json')['policies']
        tos_policy = next((policy for policy in policies if policy['handle'] == 'terms-of-service'), None)
        
        if not tos_policy:
            return {'title': None, 'body': None}
            
        return {
            'title': tos_policy.get('title'),
            'body': self._parse_policy_content(tos_policy.get('body'))
        }

    def read_shipping_policy(self):
        """
        Retrieve and parse the shipping policy.
        
        Returns:
            dict: Shipping policy title and clean text content
        """
        policies = self.read('policies.json')['policies']
        shipping_policy = next((policy for policy in policies if policy['handle'] == 'shipping-policy'), None)
        
        if not shipping_policy:
            return {'title': None, 'body': None}
            
        return {
            'title': shipping_policy.get('title'),
            'body': self._parse_policy_content(shipping_policy.get('body'))
        }
