import asyncio
import itertools
import os
import pickle

from nltk.lm import MLE
from nltk.lm.vocabulary import _dispatched_lookup, _string_lookup
from nltk.util import ngrams

from hakuin.utils import DIR_MODELS, Symbol, to_chars



_MODELS = {
    'columns': None,
    'tables': None,
    'schemas': None,
}


# NLTK models rely on vocabularies that do not support bytes and Symbols so we need to add
# them manually.
@_dispatched_lookup.register(bytes)
def _(word, vocab):
    return _string_lookup(word, vocab)


@_dispatched_lookup.register(Symbol)
def _(word, vocab):
    return _string_lookup(word, vocab)



class Model:
    '''N-gram language model.'''
    def __init__(self, order):
        '''Constructor.

        Params:
            order (int): model order
        '''
        self.model = MLE(order)
        self._lock = asyncio.Lock()


    async def predict(self, buffer):
        '''Calculates likelihood distribution of next character.

        Params:
            buffer (str|bytes): preceding characters

        Returns:
            dict: likelihood distribution
        '''
        async def _predict(context):
            async with self._lock:
                context = self.model.vocab.lookup(context)
                counts = self.model.context_counts(context or None)
            return {c: counts.freq(c) for c in counts}


        if self.model.order == 1:
            return await _predict(context=[])

        context = list(self.tokenize(buffer, add_eos=False))
        context = context[-(self.model.order - 1):]

        probs = None
        while context and not probs:
            probs = await _predict(context=context)
            context.pop(0)

        return probs or await _predict([])


    async def fit_data(self, data):
        '''Trains the model on the whole training set.

        Params:
            data (list): taining set
        '''
        train, vocab = self.padded_everygram_pipeline(data, order=self.model.order)
        await self._fit(train=train, vocab=vocab)


    async def fit_char(self, char, buffer):
        '''Trains the model with a single character.

        Params:
            char (str|bytes|Symbol): character to train
            buffer (str|bytes): preceding characters
        '''
        tokens = self.tokenize(buffer, add_eos=False)
        tokens.append(char)
        tokens = tokens[-self.model.order:]
        everygrams = self.everygrams(tokens=tokens, order=self.model.order)
        await self._fit(train=[everygrams], vocab=tokens)


    async def _fit(self, train, vocab):
        '''Trains the model.

        Params:
            train (generator): training ngrams
            vocab (itertools.chain): vocabulary
        '''
        async with self._lock:
            self.model.vocab.update(vocab)
            self.model.counts.update(self.model.vocab.lookup(t) for t in train)


    @staticmethod
    def tokenize(s, add_sos=True, add_eos=True):
        '''Converts string to a list of tokens.

        Params:
            s (str|bytes): string to tokenize
            add_sos (bool): True if SOS should be included
            add_eos (bool): True if EOS should be included

        Returns:
            list: tokens
        '''
        tokens = to_chars(s)
        if add_sos:
            tokens.insert(0, Symbol.SOS)
        if add_eos:
            tokens.append(Symbol.EOS)
        return tokens


    @staticmethod
    def everygrams(tokens, order):
        '''NLTK's everygrams is bugged (it generates multiple SOS/EOS unigrams for each sample),
            so we need to have our own implementation.

        Params:
            tokens (list): tokens to generate everygrams from
            order (int): highest order of ngrams

        Returns:
            generator: everygrams
        '''
        for n in range(1, order + 1):
            for ngram in ngrams(tokens, n=n):
                if not all(token == Symbol.SOS for token in ngram):
                    yield ngram


    @staticmethod
    def padded_everygram_pipeline(data, order):
        '''NLTK's padded_everygram_pipeline relies on the bugged everygrams function, so we need
            to have our own implementation.

        Params:
            data (list): training set
            order (int): highest order of ngrams

        Returns:
            generator, itertools.chain: training ngrams and vocabulary
        '''
        train = (Model.everygrams(tokens=Model.tokenize(d), order=order) for d in data)
        vocab = itertools.chain.from_iterable(map(Model.tokenize, data))
        return train, vocab



def get_model(name):
    '''Retrieves a pretrained model.

    Params:
        name (str): name of the model

    Returns:
        Model: model
    '''
    assert name in _MODELS, f'Model "{name}" not supported.'

    if _MODELS[name] is None:
        with open(os.path.join(DIR_MODELS, f'model_{name}.pkl'), 'rb') as f:
            _MODELS[name] = pickle.load(f)    

    return _MODELS[name]
