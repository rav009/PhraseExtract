#script action initialize the nodes environment

sudo chmod -R 777 /usr/local/lib/python2.7
pip install nltk


echo 'import nltk' > nltklib.py
echo "nltk.download('punkt')">>nltklib.py
python nltklib.py
rm nltklib.py
