import os
import requests
import json
from openai import OpenAI
import anthropic
from decouple import Config, RepositoryEnv

# Constantes de modelos soportados
SUPPORTED_CLAUDE_MODELS = [
    'claude-3-5-sonnet-20240620',
    'claude-3-5-sonnet-20241022',
    'claude-3-5-haiku-20241022',
    'claude-3-haiku-20240307',
    'claude-3-opus-20240229'
]

SUPPORTED_GPT_MODELS = [
    'gpt-4o',
    'gpt-4o-2024-08-06',
    'gpt-4o-mini',
    'text-embedding-3-large',
    'text-embedding-3-small',
    'text-embedding-ada-002'
]

class LLMBatch:
    def __init__(self):
        # Configuración del archivo .env
        env_path = "/home/notorios/Notorios/Libraries/LLM_lib/.env"
        self.env_config = Config(RepositoryEnv(env_path))
        
        # Inicializar clientes
        self._init_openai()
        self._init_anthropic()

    # Métodos de inicialización
    def _init_openai(self):
        self.openai_key = self.env_config('API_KEY')
        self.openai_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.openai_key}"
        }
        self.openai_base_url = "https://api.openai.com/v1"

    def _init_anthropic(self):
        self.anthropic_key = self.env_config('ANTHROPIC_API_KEY')
        self.claude_client = anthropic.Anthropic(api_key=self.anthropic_key)

    # Método principal de procesamiento
    def process_batch(self, requests, metadata=None):
        """
        Procesa una lista de requests en batch
        Retorna el batch_id para seguimiento
        """
        try:
            # Verificar que todos los requests usen el mismo modelo
            models = set(req.get("model") for req in requests)
            if len(models) != 1:
                raise ValueError("Todos los requests deben usar el mismo modelo")
            model = next(iter(models))
            
            # Validar modelo
            model_type = self._validate_model(model)

            # Crear y subir archivo JSONL
            input_file = self.create_batch_file(requests)
            print(f"Archivo batch creado: {input_file}")
            
            file_response = self.upload_batch_file(input_file, model)
            if 'id' not in file_response:
                print("Error en la respuesta de upload_batch_file:", file_response)
                raise Exception("No se pudo obtener el ID del archivo subido")
            print(f"Archivo subido con ID: {file_response['id']}")
            
            # Crear batch
            batch_response = self.create_batch(file_response['id'], model, metadata)            
            if 'id' not in batch_response:
                print("Error en la respuesta de create_batch:", batch_response)
                raise Exception("No se pudo obtener el ID del batch")
                
            return batch_response['id']
        except Exception as e:
            print(f"Error en process_batch: {str(e)}")
            raise

    # Métodos de creación de archivos batch
    def create_batch_file(self, requests, output_file="batch_input.jsonl"):
        """Crea un archivo JSONL para procesar en batch con validaciones"""
        # OpenAI permite 50,000 requests, Claude 100,000
        if requests.get("model").startswith("claude"):
            max_requests = 100000
        else:
            max_requests = 50000
        
        if len(requests) > max_requests:
            raise ValueError(f"Máximo {max_requests} requests por batch. Recibidos: {len(requests)}")
        
        models = set(req.get("model") for req in requests)
        if len(models) > 1:
            raise ValueError(f"Todos los requests deben usar el mismo modelo. Modelos encontrados: {models}")

        model = next(iter(models))
        
        with open(output_file, 'w') as f:
            if model in ['gpt-4o-mini', 'gpt-4o']:
                self._create_openai_batch_entries(requests, f)
            elif model in ['claude-3-5-sonnet-20240620', 'claude-3-5-sonnet']:
                self._create_claude_batch_entries(requests, f)
            
        # Validar límites según documentación
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        if self._validate_model(model) == "claude":
            if file_size_mb > 256:  # Claude: 256 MB
                raise ValueError(f"El archivo supera el límite de 256MB para Claude")
            if len(requests) > 100000:
                raise ValueError("Claude permite máximo 100,000 requests por batch")
        else:  # OpenAI
            if file_size_mb > 200:  # OpenAI: 200 MB
                raise ValueError(f"El archivo supera el límite de 200MB para OpenAI")
            if len(requests) > 50000:
                raise ValueError("OpenAI permite máximo 50,000 requests por batch")
        
        return output_file

    def _create_openai_batch_entries(self, requests, file):
        for i, request in enumerate(requests):
            batch_request = {
                "custom_id": f"request-{i}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": request.get("model", "gpt-4o"),
                    "messages": request["messages"],
                    "max_tokens": request.get("max_tokens", 1000),
                    "temperature": request.get("temperature", 0.7)
                }
            }
            file.write(json.dumps(batch_request) + '\n')

    def _create_claude_batch_entries(self, requests, file):
        """
        Formato correcto para Claude según documentación:
        {
            "custom_id": "unique-id",
            "params": {
                "model": "claude-3-5-sonnet",
                "messages": [
                    {"role": "user", "content": "Hello!"}
                ],
                "max_tokens": 1000,
                "temperature": 0.7
            }
        }
        """
        for i, request in enumerate(requests):
            messages = request["messages"]
            # Validar formato de mensajes para Claude
            for msg in messages:
                if msg["role"] == "system":
                    # Claude maneja system messages de manera diferente
                    messages = [{"role": "system", "content": msg["content"]}] + [
                        m for m in messages if m["role"] != "system"
                    ]
                    break

            batch_request = {
                "custom_id": f"request-{i}",
                "params": {
                    "model": request.get("model"),
                    "messages": messages,
                    "max_tokens": request.get("max_tokens", 1000),
                    "temperature": request.get("temperature", 0.7)
                }
            }
            file.write(json.dumps(batch_request) + '\n')

    # Métodos de gestión de batches
    def upload_batch_file(self, file_path, model):
        """Sube el archivo JSONL para procesamiento en batch"""
        if model in ['gpt-4o-mini', 'gpt-4o']:
            return self._upload_openai_batch(file_path)
        elif model in ['claude-3-5-sonnet-20240620', 'claude-3-5-sonnet']:
            return self._upload_claude_batch(file_path)
        else:
            raise ValueError(f"Modelo no soportado para procesamiento en batch: {model}")

    def create_batch(self, input_file_id, model, metadata=None):
        """Crea un nuevo batch job"""
        if model in ['gpt-4o-mini', 'gpt-4o']:
            return self._create_openai_batch(input_file_id, metadata)
        elif model in ['claude-3-5-sonnet-20240620', 'claude-3-5-sonnet']:
            return self._create_claude_batch(input_file_id, metadata)

    def get_batch_status(self, batch_id, model):
        """Obtiene el estado detallado de un batch"""
        if model in ['gpt-4o-mini', 'gpt-4o']:
            return self._get_openai_batch_status(batch_id)
        elif model in ['claude-3-5-sonnet-20240620', 'claude-3-5-sonnet']:
            return self._get_claude_batch_status(batch_id)

    def get_batch_results(self, batch_id, model):
        """
        Obtiene los resultados de un batch completado.
        Los resultados están disponibles por 29 días para Claude y 24h para OpenAI
        """
        status = self.get_batch_status(batch_id, model)
        
        if self._validate_model(model) == "claude":
            if status.get("processing_status") != "ended":
                raise ValueError("El batch aún no ha terminado")
            
            results_url = status.get("results_url")
            if not results_url:
                raise ValueError("No se encontró URL de resultados")
            
            response = requests.get(
                results_url,
                headers={
                    "x-api-key": self.anthropic_key,
                    "anthropic-version": "2024-01-01"
                }
            )
        else:  # OpenAI
            if status.get("status") != "completed":
                raise ValueError("El batch aún no ha terminado")
            
            output_file_id = status.get("output_file_id")
            response = requests.get(
                f"{self.openai_base_url}/files/{output_file_id}/content",
                headers=self.openai_headers
            )
        
        response.raise_for_status()
        return response.text  # Retorna el contenido JSONL

    # Métodos específicos de OpenAI
    def _upload_openai_batch(self, file_path):
        with open(file_path, 'rb') as f:
            response = requests.post(
                f"{self.openai_base_url}/files",
                headers={"Authorization": f"Bearer {self.openai_key}"},
                files={"file": f},
                data={"purpose": "batch"}
            )
        return response.json()

    def _create_openai_batch(self, input_file_id, metadata=None):
        data = {
            "input_file_id": input_file_id,
            "endpoint": "/v1/chat/completions",
            "completion_window": "24h"
        }
        if metadata:
            data["metadata"] = metadata

        try:
            response = requests.post(
                f"{self.openai_base_url}/batches",
                headers=self.openai_headers,
                json=data
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error en la solicitud HTTP: {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"Respuesta del servidor: {e.response.text}")
            raise

    def _get_openai_batch_status(self, batch_id):
        try:
            response = requests.get(
                f"{self.openai_base_url}/batches/{batch_id}",
                headers=self.openai_headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener estado del batch: {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"Respuesta del servidor: {e.response.text}")
            raise

    # Métodos específicos de Claude
    def _upload_claude_batch(self, file_path):
        # Claude no necesita un paso de upload separado
        return {"id": file_path}

    def _create_claude_batch(self, input_file_id, metadata=None):
        with open(input_file_id, 'r') as f:
            requests = [json.loads(line) for line in f]
        
        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages/batches",
                headers={
                    "x-api-key": self.anthropic_key,
                    "anthropic-version": "2024-01-01",
                    "content-type": "application/json"
                },
                json={
                    "requests": requests,
                    "metadata": metadata if metadata else {}
                }
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error en la solicitud HTTP: {str(e)}")
            raise

    def _get_claude_batch_status(self, batch_id):
        try:
            response = requests.get(
                f"https://api.anthropic.com/v1/message_batches/{batch_id}",
                headers={
                    "x-api-key": self.anthropic_key,
                    "anthropic-version": "2024-01-01",
                    "content-type": "application/json"
                }
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener estado del batch: {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"Respuesta del servidor: {e.response.text}")
            raise

    def _validate_model(self, model):
        """Valida que el modelo esté soportado"""
        if model in SUPPORTED_CLAUDE_MODELS:
            return "claude"
        elif model in SUPPORTED_GPT_MODELS:
            return "gpt"
        else:
            raise ValueError(f"Modelo no soportado: {model}. Modelos soportados: \nClaude: {SUPPORTED_CLAUDE_MODELS}\nGPT: {SUPPORTED_GPT_MODELS}")

    def cancel_batch(self, batch_id, model):
        """Cancela un batch en progreso"""
        if self._validate_model(model) == "claude":
            response = requests.post(
                f"https://api.anthropic.com/v1/message_batches/{batch_id}/cancel",
                headers={
                    "x-api-key": self.anthropic_key,
                    "anthropic-version": "2024-01-01"
                }
            )
        else:  # OpenAI
            response = requests.post(
                f"{self.openai_base_url}/batches/{batch_id}/cancel",
                headers=self.openai_headers
            )
        
        response.raise_for_status()
        return response.json()
