FROM continuumio/anaconda3

ADD . .
RUN conda install -y spacy
RUN git clone https://github.com/titipata/pubmed_parser.git
RUN pip install -r pubmed_parser/requirements.txt
RUN cd pubmed_parser && python setup.py install
CMD ["python", "run.py"]
