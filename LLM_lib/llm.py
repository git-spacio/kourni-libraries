import requests
import anthropic
from openai import OpenAI
from decouple import Config, RepositoryEnv
import voyageai
import google.generativeai as genai
from io import BytesIO
import requests
from PIL import Image
import base64


class LLM:
    def __init__(self):
        # Configuración del archivo .env
        env_path = "/home/snparada/Spacionatural/Libraries/LLM_lib/.env"
        self.env_config = Config(RepositoryEnv(env_path))
        self._init_openai()
        self._init_anthropic()
        self._init_perplexity()
        self._init_voyage()
        self._init_gemini()
        
    def _init_openai(self):
        self.openai_key = self.env_config('API_KEY')
        self.openai_client = OpenAI(api_key=self.openai_key)
        self.openai_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.openai_key}"
        }
        self.openai_base_url = "https://api.openai.com/v1"

    def _init_anthropic(self):
        self.anthropic_key = self.env_config('ANTHROPIC_API_KEY')
        self.claude_client = anthropic.Anthropic(api_key=self.anthropic_key)

    def _init_perplexity(self):
        self.perplexity_key = self.env_config('PERPLEXITY_API_KEY')
        self.perplexity_client = OpenAI(
            api_key=self.perplexity_key,
            base_url="https://api.perplexity.ai"
        )

    def _init_voyage(self):
        self.voyage_key = self.env_config('ANTHROPIC_API_KEY')
        self.voyage_client = voyageai.Client(api_key=self.voyage_key)

    def _init_gemini(self):
        self.gemini_key = self.env_config('GEMINI_API_KEY')
        genai.configure(api_key=self.gemini_key)

    def generate_embedding(self, text, model="text-embedding-3-small"):
        # Modelos OpenAI
        if model in ['text-embedding-3-large', 'text-embedding-3-small']:
            return self._get_openai_embedding(text, model)
        # Modelos Voyage
        elif model in ['voyage-2', 'voyage-large-2']:
            return self._get_voyage_embedding(text, model)
        else:
            raise ValueError(f"Modelo no soportado: {model}")

    def generate_text(self, content, model="gpt-4o-mini", max_tokens=1024, temperature=0.7, stream=False):
        # Modelos de OpenAI
        if model in ['gpt-4o-mini', 'gpt-4o','o1-mini','o1','o1-mini-2024-09-12']:
            return self._generate_with_openai(content, model, max_tokens, temperature)
            
        # Modelos de Claude
        elif model in ['claude-3-5-sonnet-20241022', 'claude-3-5-sonnet','claude-3-5-haiku-20241022']:
            return self._generate_with_claude(content, model, max_tokens, temperature)
            
        # Modelos de Perplexity
        elif model == "llama-3.1-sonar-large-128k-online":
            return self._generate_with_perplexity(content, model, stream)
            
        # Modelos de Gemini
        elif model in ['gemini-1.5-flash', 'gemini-1.5-pro']:
            return self._generate_with_gemini(content, model, max_tokens, temperature, stream)
            
        else:
            raise ValueError(f"Modelo no soportado: {model}")

    def generate_by_image_or_text(self, content, image_path=None, image_url=None, model="gemini-1.5-pro", max_tokens=1024, temperature=0.7, stream=False):
        """
        Genera respuestas basadas en texto o imagen + texto.
        
        Args:
            content (str): El texto de la prompt
            image_path (str, optional): Ruta local a la imagen
            image_url (str, optional): URL de la imagen
            model (str): Modelo a utilizar
            max_tokens (int): Número máximo de tokens
            temperature (float): Temperatura para la generación
            stream (bool): Si se debe transmitir la respuesta
        """
        try:
            # Si tenemos una imagen (ya sea URL o path local)
            if image_path or image_url:
                # Descargar la imagen si es una URL
                if image_url:
                    import requests
                    from io import BytesIO
                    
                    response = requests.get(image_url)
                    image_data = BytesIO(response.content)
                else:
                    image_data = image_path

                if model.startswith('gemini'):
                    return self._generate_with_gemini_vision(content, image_data, model, max_tokens, temperature, stream)
                elif model.startswith('claude'):
                    return self._generate_with_claude_vision(content, image_data, model, max_tokens, temperature)
                else:
                    raise ValueError(f"El modelo {model} no soporta procesamiento de imágenes")
            
            # Si solo es texto, usamos la función generate_text existente
            return self.generate_text(content, model, max_tokens, temperature, stream)
            
        except Exception as err:
            print(f"Error en generate_by_image_or_text: {err}")
            return None

#----------------------------------TEXT----------------------------------

    def _generate_with_openai(self, content, model, max_tokens, temperature):
        # Configuración base
        data = {
            "model": model,
            "messages": [{"role": "user", "content": content}],
        }
        
        # Configuración específica según el modelo
        if model in ['o1', 'o1-mini', 'o1-mini-2024-09-12']:
            # Para modelos O1, solo usamos max_completion_tokens
            data["max_completion_tokens"] = int(max_tokens)
        else:
            # Para otros modelos de OpenAI, usamos max_tokens y temperature
            data["max_tokens"] = int(max_tokens)
            data["temperature"] = float(temperature)

        try:
            response = requests.post(
                f"{self.openai_base_url}/chat/completions",
                headers=self.openai_headers,
                json=data
            )
            response.raise_for_status()
            response_data = response.json()

            if "choices" in response_data and len(response_data["choices"]) > 0:
                return response_data["choices"][0]["message"]["content"].strip()
            else:
                print("No se encontraron 'choices' en la respuesta de GPT.")
                return None
        except Exception as err:
            print(f"Error en OpenAI: {err}")
            return None

    def _generate_with_claude(self, content, model, max_tokens, temperature):
        try:
            messages = [{"role": "user", "content": content}]
            response = self.claude_client.messages.create(
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                messages=messages
            )
            return response.content[0].text
        except Exception as err:
            print(f"Error en Claude: {err}")
            return None

    def _generate_with_perplexity(self, content, model, stream=False):
        messages = [
            {"role": "system", "content": "Se preciso y conciso."},
            {"role": "user", "content": content}
        ]

        try:
            if not stream:
                response = self.perplexity_client.chat.completions.create(
                    model=model,
                    messages=messages
                )
                return response.choices[0].message.content.strip()
            else:
                return self.perplexity_client.chat.completions.create(
                    model=model,
                    messages=messages,
                    stream=True
                )
        except Exception as err:
            print(f"Error en Perplexity: {err}")
            return None

    def _generate_with_gemini(self, content, model, max_tokens, temperature, stream=False):
        try:
            gemini_model = genai.GenerativeModel(model)
            
            # Configurar los parámetros de generación
            generation_config = genai.types.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=temperature
            )
            
            # Generar respuesta
            if not stream:
                response = gemini_model.generate_content(
                    content,
                    generation_config=generation_config
                )
                return response.text
            else:
                return gemini_model.generate_content(
                    content,
                    generation_config=generation_config,
                    stream=True
                )
                
        except Exception as err:
            print(f"Error en Gemini: {err}")
            return None


#----------------------------------EMBEDDINGS----------------------------------

    def _get_openai_embedding(self, text, model):
        try:
            text = text.replace("\n", " ")
            response = self.openai_client.embeddings.create(
                input=[text],
                model=model
            )
            return response.data[0].embedding
        except Exception as err:
            print(f"Error en OpenAI Embeddings: {err}")
            return None

    def _get_voyage_embedding(self, text, model, input_type=None):
        try:
            result = self.voyage_client.embed(
                [text],
                model=model,
                input_type=input_type
            )
            return result.embeddings[0]
        except Exception as err:
            print(f"Error en Voyage Embeddings: {err}")
            return None

#----------------------------------VISION----------------------------------

    def _generate_with_gemini_vision(self, content, image_data, model, max_tokens, temperature, stream=False):
        try:
            import PIL.Image
            
            # Cargar la imagen
            if isinstance(image_data, str):
                image = PIL.Image.open(image_data)
            else:
                image = PIL.Image.open(image_data)
            
            # Configurar el modelo
            gemini_model = genai.GenerativeModel(model)
            generation_config = genai.types.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=temperature
            )
            
            # Generar respuesta
            if not stream:
                response = gemini_model.generate_content(
                    [content, image],
                    generation_config=generation_config
                )
                return response.text
            else:
                return gemini_model.generate_content(
                    [content, image],
                    generation_config=generation_config,
                    stream=True
                )
                
        except Exception as err:
            print(f"Error en Gemini Vision: {err}")
            return None

    def _generate_with_claude_vision(self, content, image_data, model, max_tokens, temperature):
        try:
            # Add required imports at the beginning of the method
            from io import BytesIO
            import requests
            from PIL import Image
            import base64

            # Map model names to their vision-capable versions
            vision_model_mapping = {
                'claude-3-5-sonnet': 'claude-3-5-sonnet-20241022',
                'claude-3-5-haiku': 'claude-3-5-haiku-20241022'
            }
            model_to_use = vision_model_mapping.get(model, model)
            
            # Handle image data
            if isinstance(image_data, str):
                if image_data.startswith(('http://', 'https://')):
                    import requests
                    from PIL import Image
                    from io import BytesIO
                    
                    # Download image
                    response = requests.get(image_data, verify=False)
                    if response.status_code != 200:
                        raise Exception(f"Failed to download image. Status code: {response.status_code}")
                    
                    # Open with PIL to handle format conversion if needed
                    img = Image.open(BytesIO(response.content))
                    
                    # Convert to RGB if necessary (handles RGBA PNG)
                    if img.mode in ('RGBA', 'P'):
                        img = img.convert('RGB')
                    
                    # Save to BytesIO in JPEG format
                    output = BytesIO()
                    img.save(output, format='JPEG', quality=95)
                    image_bytes = output.getvalue()
                    media_type = 'image/jpeg'
                else:
                    # Handle local file
                    from PIL import Image
                    img = Image.open(image_data)
                    if img.mode in ('RGBA', 'P'):
                        img = img.convert('RGB')
                    output = BytesIO()
                    img.save(output, format='JPEG', quality=95)
                    image_bytes = output.getvalue()
                    media_type = 'image/jpeg'
            else:
                # Handle BytesIO input
                from PIL import Image
                img = Image.open(image_data)
                if img.mode in ('RGBA', 'P'):
                    img = img.convert('RGB')
                output = BytesIO()
                img.save(output, format='JPEG', quality=95)
                image_bytes = output.getvalue()
                media_type = 'image/jpeg'

            # Encode to base64
            import base64
            base64_image = base64.b64encode(image_bytes).decode('utf-8')
            
            # Create message with correct structure
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": base64_image
                            }
                        },
                        {
                            "type": "text",
                            "text": content
                        }
                    ]
                }
            ]
            
            response = self.claude_client.messages.create(
                model=model_to_use,
                max_tokens=max_tokens,
                temperature=temperature,
                messages=messages
            )
            return response.content[0].text
            
        except Exception as err:
            print(f"Error en Claude Vision: {err}")
            import traceback
            print(traceback.format_exc())
            return None
