import asyncio
import itertools
import os
import pickle

from nltk.lm import MLE
from nltk.util import ngrams

from hakuin.utils import DIR_MODELS, SOS, EOS, tokenize



_MODELS = {
    'columns': None,
    'tables': None,
    'schemas': None,
}


class Model:
    '''N-gram language model.'''
    def __init__(self, order):
        '''Constructor.

        Params:
            order (int): model order
        '''
        self.model = MLE(order)
        self._type = None
        self._lock = asyncio.Lock()


    async def scores(self, context):
        '''Calculates likelihood distribution of next character.

        Params:
            context (list): model context

        Returns:
            dict: likelihood distribution
        '''
        context = context[-(self.model.order - 1):] if self.model.order > 1 else []
        context = [self._serialize(v) for v in context]

        while context:
            scores = await self._scores(context)
            if scores:
                return scores
            context.pop(0)

        return await self._scores([])


    async def _scores(self, context):
        async with self._lock:
            context = self.model.vocab.lookup(context) if context else None
            counts = self.model.context_counts(context)

        return {self._deserialize(c): counts.freq(c) for c in counts}


    async def fit(self, value):
        '''TODO'''
        await self.fit_data(data=[value])


    async def fit_data(self, data):
        '''TODO'''
        train, vocab = self._everygrams(data)
        await self._fit(train=train, vocab=vocab)


    async def fit_char(self, char, buffer):
        '''TODO'''
        tokens = tokenize(buffer, add_eos=False) + [char]
        tokens = tokens[-self.model.order:]
        tokens = [self._serialize(t) for t in tokens]
        egrams = (tokens[i:] for i in range(len(tokens)))
        await self._fit(train=[egrams], vocab=tokens)


    async def _fit(self, train, vocab):
        async with self._lock:
            self.model.vocab.update(vocab)
            self.model.counts.update(self.model.vocab.lookup(t) for t in train)


    def _everygrams(self, data):
        '''Creates character-based train set and vocabulary.

        Params:
            data (list): train set strings

        Returns:
            (generator, itertools.chain): train set and vocabulary
        '''
        def _ngrams(s, order):
            tokens = [self._serialize(t) for t in tokenize(s)]
            return ngrams(tokens, n=order, pad_left=False, pad_right=False)

        def _egrams(s):
            return (ngram for i in range(1, self.model.order + 1) for ngram in _ngrams(s, i))

        train = (_egrams(s) for s in data)
        vocab = itertools.chain.from_iterable(map(tokenize, data))
        vocab = (self._serialize(t) for t in vocab)
        return train, vocab


    def _serialize(self, value):
        '''NLTK models support only strings. As a workaround, we serialize items as strings and preserve
        their original data type for deserialization.

        Params:
            value (value): value to be serialzied

        Returns:
            str: serialzied value
        '''
        if value in [SOS, EOS]:
            return value

        assert type(value) in [str, bytes], f'Type cannot be serialized: {type(value)}'
        if self._type is None:
            self._type = type(value)

        return bytes.hex(value) if self._type is bytes else value


    def _deserialize(self, value):
        '''See _serialize.

        Params:
            value (str): value to be deserialzied

        Returns:
            value: deserialzied value
        '''
        if value in [SOS, EOS]:
            return value

        return bytes.fromhex(value) if self._type is bytes else value



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