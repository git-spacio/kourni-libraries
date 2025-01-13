from datetime import datetime
from .api import OdooAPI
import pandas as pd

class OdooCRM(OdooAPI):

    def __init__(self, database='productive'):
        super().__init__(database=database)
    
    def create_oportunity(self, data):
        """
        Crea una nueva oportunidad en el CRM
        """
        try:
            print(f"\nIntentando crear oportunidad con datos: {data}")
            
            # Campos mínimos requeridos para crear una oportunidad
            required_fields = {
                'name': data.get('name'),
                'partner_id': data.get('partner_id'),
                'expected_revenue': data.get('expected_revenue', 0.0),
                'probability': data.get('probability', 0.0),
                'type': 'opportunity',
            }
            
            # Campos opcionales comunes
            optional_fields = {
                'team_id': data.get('team_id'),
                'user_id': data.get('user_id'),
                'description': data.get('description'),
                'date_deadline': data.get('date_deadline'),
                'priority': data.get('priority', '1'),
                'tag_ids': [(6, 0, data.get('tag_ids', []))],
            }
            
            # Combinar campos y filtrar los valores None
            opportunity_data = {**required_fields, **optional_fields}
            opportunity_data = {k: v for k, v in opportunity_data.items() if v is not None}
            
            print(f"\nDatos finales para crear oportunidad: {opportunity_data}")
            
            # Crear la oportunidad
            opportunity_id = self.models.execute_kw(
                self.db, self.uid, self.password,
                'crm.lead', 'create',
                [opportunity_data]
            )
            
            print(f"\nID de oportunidad creada: {opportunity_id}")
            
            # Verificar inmediatamente después de crear
            created_opp = self.models.execute_kw(
                self.db, self.uid, self.password,
                'crm.lead', 'read',
                [opportunity_id],
                {'fields': ['name', 'type', 'partner_id']}
            )
            print(f"\nVerificación inmediata de la oportunidad creada: {created_opp}")
            
            return opportunity_id
            
        except Exception as e:
            print(f"\nError detallado al crear oportunidad: {str(e)}")
            return f"Error al crear la oportunidad: {str(e)}"

    def create_quotation_from_opportunity(self, opportunity_id, order_lines):
        """
        Crea una cotización desde una oportunidad usando el método nativo de Odoo
        
        :param opportunity_id: ID de la oportunidad
        :param order_lines: Lista de diccionarios con los productos
            [
                {
                    'product_id': 123,      # ID del producto
                    'product_uom_qty': 1.0,  # Cantidad
                },
                ...
            ]
        :return: ID de la cotización creada o mensaje de error
        """
        try:
            # 1. Obtener información de la oportunidad
            opportunity = self.models.execute_kw(
                self.db, self.uid, self.password,
                'crm.lead', 'read',
                [opportunity_id],
                {'fields': ['partner_id', 'team_id', 'user_id']}
            )[0]
            
            # 2. Crear la cotización directamente
            quotation_data = {
                'partner_id': opportunity['partner_id'][0],
                'opportunity_id': opportunity_id,
                'team_id': opportunity.get('team_id', False) and opportunity['team_id'][0],
                'user_id': opportunity.get('user_id', False) and opportunity['user_id'][0],
                'state': 'draft',
            }
            
            sale_order_id = self.models.execute_kw(
                self.db, self.uid, self.password,
                'sale.order', 'create',
                [quotation_data]
            )
            
            if not sale_order_id:
                return "Error: No se pudo crear la cotización"
            
            # 3. Agregar las líneas de producto
            for line in order_lines:
                # Obtener información completa del producto y sus variantes
                product_info = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    'product.product', 'read',
                    [line['product_id']],
                    {'fields': ['uom_id', 'name', 'list_price', 'price_extra']}
                )[0]
                
                # Calcular el precio final considerando el precio extra de la variante
                final_price = product_info['list_price'] + (product_info.get('price_extra', 0.0) or 0.0)
                
                line_data = {
                    'order_id': sale_order_id,
                    'product_id': line['product_id'],
                    'product_uom_qty': line.get('product_uom_qty', 1.0),
                    'product_uom': product_info['uom_id'][0],
                    'name': product_info['name'],
                    'price_unit': line.get('price_unit', final_price),  # Usamos el precio final calculado
                }
                
                line_id = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    'sale.order.line', 'create',
                    [line_data]
                )
                
                if not line_id:
                    return f"Error: No se pudo crear la línea para el producto {product_info['name']}"
            
            return sale_order_id
            
        except Exception as e:
            return f"Error al crear la cotización: {str(e)}"
    
    def read_oportunity_by_id(self, id):
        """
        Lee una oportunidad específica por su ID
        
        :param id: ID de la oportunidad
        :return: DataFrame con los datos de la oportunidad o mensaje de error
        """
        try:
            fields = [
                'name',
                'partner_id',
                'expected_revenue',
                'probability',
                'team_id',
                'user_id',
                'description',
                'date_deadline',
                'priority',
                'tag_ids',
                'stage_id',
                'create_date',
                'write_date',
                'email_from',
                'phone',
            ]
            
            opportunity = self.models.execute_kw(
                self.db, self.uid, self.password,
                'crm.lead', 'read',
                [id],
                {'fields': fields}
            )
            
            if opportunity:
                df = pd.DataFrame([opportunity[0]])
                return df
            else:
                return "Oportunidad no encontrada"
            
        except Exception as e:
            return f"Error al leer la oportunidad: {str(e)}"

    def read_all_opportunities_in_df(self, domain=None, limit=None):
        """
        Lee todas las oportunidades que coincidan con el dominio especificado
        
        :param domain: Lista de condiciones para filtrar las oportunidades (opcional)
        :param limit: Número máximo de registros a retornar (opcional)
        :return: DataFrame con las oportunidades o mensaje de error
        """
        try:
            # Campos a recuperar
            fields = [
                'name',
                'partner_id',
                'expected_revenue',
                'probability',
                'team_id',
                'user_id',
                'description',
                'date_deadline',
                'priority',
                'tag_ids',
                'stage_id',
                'create_date',
                'write_date',
                'email_from',
                'phone',
                'order_ids',
            ]
            
            # Si no se especifica un dominio, usar lista vacía
            domain = domain or []
            
            # Buscar IDs de oportunidades
            opportunity_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'crm.lead', 'search',
                [domain],
                {'limit': limit} if limit else {}
            )
            
            if not opportunity_ids:
                return "No se encontraron oportunidades"
            
            # Leer datos de las oportunidades
            opportunities = self.models.execute_kw(
                self.db, self.uid, self.password,
                'crm.lead', 'read',
                [opportunity_ids],
                {'fields': fields}
            )
            
            # Convertir a DataFrame
            df = pd.DataFrame(opportunities)
            return df
            
        except Exception as e:
            return f"Error al leer las oportunidades: {str(e)}"

    def update_oportunity_by_id(self, id, data):
        """
        Actualiza una oportunidad existente por su ID
        
        :param id: ID de la oportunidad a actualizar
        :param data: diccionario con los campos a actualizar
        :return: True si la actualización fue exitosa o mensaje de error
        """
        try:
            # Verificar que la oportunidad existe
            existing = self.models.execute_kw(
                self.db, self.uid, self.password,
                'crm.lead', 'search',
                [[['id', '=', id]]]
            )
            
            if not existing:
                return "Oportunidad no encontrada"
            
            # Actualizar la oportunidad
            self.models.execute_kw(
                self.db, self.uid, self.password,
                'crm.lead', 'write',
                [[id], data]
            )
            
            return True
            
        except Exception as e:
            return f"Error al actualizar la oportunidad: {str(e)}"
    
