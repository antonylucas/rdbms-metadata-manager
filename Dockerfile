# Usar uma imagem base leve do Python
FROM python:3.10-slim

# Definir o diretório de trabalho no container
WORKDIR /app

# Copiar o arquivo de dependências para o container
COPY requirements.txt .

# Instalar as dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Instalar o python-dotenv para gerenciar variáveis de ambiente
RUN pip install python-dotenv

# Copiar todo o código da aplicação para o diretório de trabalho no container
COPY . .

# Criar o diretório para exportar os relatórios CSV
RUN mkdir -p exports

# Definir a variável de ambiente para evitar buffer no output do Python
ENV PYTHONUNBUFFERED=1

# Definir o comando padrão para executar a aplicação
CMD ["python", "app.py"]
