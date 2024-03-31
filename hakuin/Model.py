import asyncio
import logging
import os
import pickle

from nltk.lm import MLE

import hakuin
import hakuin.utils.ngrams as ngrams
from hakuin.utils import tokenize, DIR_MODELS


_m_schemas = None
_m_tables = None
_m_columns = None



class Model:
    '''Everygram language model.'''
    def __init__(self, order):
        '''Constructor.

        Params:
            order (int|None): everygram order
        '''
        self.model = MLE(order) if order is not None else None
        self._lock = asyncio.Lock()


    @property
    def order(self):
        if self.model:
            return self.model.order
        return None


    def load(self, file):
        '''Loads model from file.

        Params:
            file (str): model file
        '''
        logging.info(f'Loading model "{file}".')

        with open(file, 'rb') as f:
            self.model = pickle.load(f)

        logging.info(f'Model loaded.')


    async def scores(self, context):
        '''Calculates likelihood distribution of next value.

        Params:
            context (list): model context

        Returns:
            dict: likelihood distribution
        '''
        context = [] if self.order == 1 else context[-(self.order - 1):]

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

        return {c: counts.freq(c) for c in counts}


    async def count(self, value, context):
        '''Counts the number of occurences of value in a given context

        Params:
            value (value): value to count
            context (list): model context

        Returns:
            int: value count
        '''
        async with self._lock:
            context = self.model.vocab.lookup(context) if context else None
            return self.model.context_counts(context).get(value, 0)


    async def fit_data(self, data):
        '''Splits samples in data into character-based ngrams and trains model with them.

        Params:
            data (list): list of train samples
        '''
        train, vocab = ngrams.padded_everygram_pipeline(data, self.order)
        await self._fit(train, vocab)


    async def fit_single(self, value, context):
        '''Trains model with single ngram.

        Params:
            value (value): value to be trained
            context (list): model context
        '''
        context = context + [value]
        context = context[-self.order:]

        train = (context[i:] for i in range(self.order))
        train = (train, )
        await self._fit(train, context)


    async def fit_correct_char(self, correct, partial_str):
        '''Trains model with ngram, where partially extracted string is followed
        with the correct character.

        Params:
            correct (str): correct character
            partial_str (str): partially extracted string 
        '''
        context = tokenize(partial_str, add_eos=False, pad_left=self.order)
        await self.fit_single(correct, context)


    async def _fit(self, train, vocab):
        async with self._lock:
            self.model.vocab.update(vocab)
            self.model.counts.update(self.model.vocab.lookup(t) for t in train)



def get_model_schemas():
    global _m_schemas
    if _m_schemas is None:
        _m_schemas = Model(None)
        _m_schemas.load(os.path.join(DIR_MODELS, 'model_schemas.pkl'))
    return _m_schemas


def get_model_tables():
    global _m_tables
    if _m_tables is None:
        _m_tables = Model(None)
        _m_tables.load(os.path.join(DIR_MODELS, 'model_tables.pkl'))
    return _m_tables


def get_model_columns():
    global _m_columns
    if _m_columns is None:
        _m_columns = Model(None)
        _m_columns.load(os.path.join(DIR_MODELS, 'model_columns.pkl'))
    return _m_columns
