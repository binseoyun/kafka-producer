#hook에 의해 실행
source /src/kafka_venv/bin/activate #python 가상환경 실행
python3 /src/kafka-producer/deploy/replace_secret.py
pip3 install -r /src/kafka-producer/requirements.txt #EC2 상에도 동일한 라이브러리 설치