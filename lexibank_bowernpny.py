# encoding: utf-8
from __future__ import unicode_literals, print_function
from clldutils.path import Path
from clldutils.text import split_text, strip_brackets
from clldutils.path import Path
from pylexibank.dataset import Metadata
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.lingpy_util import getEvoBibAsBibtex

import csv

import re

class Dataset(BaseDataset):
    dir = Path(__file__).parent

    def cmd_install(self, **kw):
        # Please note that we are removing asterisks from Bowern et al. data, used to
        # mark words which do not occur in the Swadesh 100 or 200 lists; after the
        # mapping from self.concept we also update with equivalent glosses (i.e.,
        # concepts with more than one label in source)
        equivalent_concepts = {
            'ashes' : 'ash',
            'bad' : 'bad/evil',
            'blowfly': 'fly',
            'pimple' : 'boil/pimple',
            'boomerang' : 'boomerang/throwing stick',
            'true' : 'correct/true',
            'cut' : 'cut/hack',
            'die' : 'die/be dead',
            'dingo' : 'dingo/wolf',
            'blunt' : 'dull/blunt',
            'earth' : 'earth/soil',
            'faeces' : 'feces',
            'fat (n)' : 'fat/grease',
            'hair of head' : 'hair',
            '1sg' : 'I',
            '2sg' : 'you.SG',
            '3sg' : 'he/she',
            '1pl.excl' : 'we.EXCL.PL',
            '1pl.incl' : 'we.INCL.PL',
            '2pl' : 'thou',
            '3pl' : 'they',
            'hit' : 'hit (with hand)',
            'how?' : 'how',
            'what?' : 'what',
            'when?' : 'when',
            'where?' : 'where',
            'who?' : 'who',
            'inside' : 'in/inside',
            'kangaroo' : 'kangaroo/deer',
            'know' : 'know/be knowledgeable',
            'be alive' : 'live/be alive',
            'man' : 'man/male',
            'meat' : 'meat/flesh',
            'near' : 'near/close',
            'open' : 'open/uncover',
            'person' : 'person/human being',
            'road' : 'road/path',
            'ashamed' : 'shy/ashamed',
            'smell (tr/intr)' : 'sniff/smell',
            'stab' : 'stab/pierce',
            'tie up' : 'tie up/fasten',
            'woman' : 'woman/female',
            'sick' : 'painful/sick',
        }
        concepticon = {
            c['ENGLISH'] : (c['CONCEPTICON_ID'], c['CONCEPTICON_GLOSS'], c['ENGLISH'])
            for c in self.concepts
        }
        concepticon.update({k:concepticon[v] for k, v in equivalent_concepts.items()})

        with self.cldf as ds:
            ds.add_sources(*self.raw.read_bib())
            # add languages to dataset and build mapping
            for lang in self.languages:
                # add to dataset
                ds.add_language(
                    ID=lang['NAME'],
                    glottocode=lang['GLOTTOCODE'],
                    name=lang['GLOTTOLOG_NAME'])

            # add concepts to dataset
            for concept in self.concepts:
                ds.add_concept(
                    ID=concept['ENGLISH'],
                    conceptset=concept['CONCEPTICON_ID'],
                    gloss=concept['CONCEPTICON_GLOSS'])

            # read source file, skipping over header ([1:])
            for i, row in enumerate(self.raw.read_tsv("bowernpny.tsv", quoting=csv.QUOTE_NONE)[1:]):
                lang_entry = row[0]
                concept_entry = concepticon[row[2]][2]
                value = row[3]
                cogid = row[4]

                # replace all commas by slashes and '\x0b' by whitespace, so as
                # not to have lots of escapes around, replace double backslashes
                # by slahes (used as separator in some doculects) and
                # remove double quotes when leading and trailing (don't want
                # to change pylexibank.dataset.Dataset) -- checked for false
                # positives
                value = value.replace(',', '/')
                value = value.replace('\x0b', ' ')
                value = value.replace('\\\\', '/')
                value = value.replace(', OR', ', ')
                value = value.replace(' OR ', ', ')
                value = value.replace(' or ', ', ')
                if value:
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]

                # correct bad entries that can be corrected
                if value in self.lexemes:
                    value = self.lexemes[value]

                # skip empty entries and purely referential information
                if value in ["only specific", "not used"]:
                    continue
                if value.startswith('refer '):
                    continue
                if not value:
                    continue

                # if not empty or different from '/' (this is a leftover from one of the
                # ways of annotating missing information, "[missing] / [missing]") and
                # '?' (another way of coding missing information)
                for form_idx, form in enumerate(split_text(value, separators=',;/~')):
                    # remove superfluous characters/strings
                    form = form.replace('“', '')
                    form = form.replace('”', '')
                    form = form.replace('<', '')
                    form = form.replace('>', '')
                    form = form.replace('CHECK', '')

                    # strip brackets, parentheses, etc.
                    form = strip_brackets(form, brackets={'[[':']]', '[':']', '{':'}', '(':')'})

                    # remove final exclamation mark in (some) interjections
                    # and question mark indicating doubt
                    if form.endswith('!'):
                        form = form[:-1]
                    if form.endswith(' ?'):
                        form = form[:-2]

                    # forms that begin and end with single quotes are quotations,
                    # not glottal stops
                    if form.startswith("'") and form.endswith("'"):
                        form = form[1:-1]

                    # remove multiple spaces, plurs leading&trailing spaces
                    form = re.sub('\s+', ' ', form.strip())

                    # if not empty and not form indicating empty
                    if form and form not in ['/', '?', '-', '—', ']']:
                        tokens = self._tokenizer('IPA', form)

                        for row in ds.add_lexemes(
                            Language_ID=lang_entry,
                            Parameter_ID=concept_entry,
                            Value=form,
                            Segments=tokens,
                            Source=['Bowern2012'],
                        ):
                            # in Bowern dataset, cogid zero seems to be reserved for all
                            # entries without a single established cognate, but they are not
                            # cognates among themselves (obviously), so we need unique
                            # IDs
                            if cogid == '0':
                                cognate_set_id = None
                            else:
                                cognate_set_id = '%s-%s' % (concept_entry, cogid)

                            ds.add_cognate(
                                lexeme=row,
                                Cognateset_ID=cognate_set_id,
                                Cognate_source=['Bowern2012'],
                                Alignment_source='List2014e')

            ds.align_cognates()


    def cmd_download(self, **kw):
        if not self.raw.exists():
            self.raw.mkdir()
        self.raw.download(
            "https://zenodo.org/record/1005671/files/Pny2012Codes.tsv",
            Path(self.raw, 'bowernpny.tsv',
            log=self.log))

        self.raw.write('sources.bib', getEvoBibAsBibtex('Bowern2012', 'List2014e', **kw))
