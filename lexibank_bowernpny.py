from clldutils.misc import slug
from pathlib import Path
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.util import getEvoBibAsBibtex
from pylexibank import FormSpec
from pylexibank import progressbar as pb
from csv import QUOTE_NONE


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "bowernpny"
    form_spec = FormSpec(
            separators = ";,~/",
            missing_data = (
                "-",
                "?",
                "—"
                ),
            brackets = {"(": ")", "{": "}", "[": "]"},
            strip_inside_brackets=True,
            replacements=[(" ", "_")],
            first_form_only=True
            )

    def cmd_makecldf(self, args):
        # corrections to the data in concepticon
        concept_replacements = {
            "ashes": "ash",
            "bad": "bad/evil",
            "blowfly": "fly",
            "pimple": "boil/pimple",
            "boomerang": "boomerang/throwing stick",
            "true": "correct/true",
            "cut": "cut/hack",
            "die": "die/be dead",
            "dingo": "dingo/wolf",
            "blunt": "dull/blunt",
            "earth": "earth/soil",
            "faeces": "feces",
            "fat (n)": "fat/grease",
            "hair of head": "hair",
            "1sg": "I",
            "2sg": "you.SG",
            "3sg": "he/she",
            "1pl.excl": "we.EXCL.PL",
            "1pl.incl": "we.INCL.PL",
            "2pl": "thou",
            "3pl": "they",
            "hit": "hit (with hand)",
            "how?": "how",
            "what?": "what",
            "when?": "when",
            "where?": "where",
            "who?": "who",
            "inside": "in/inside",
            "kangaroo": "kangaroo/deer",
            "know": "know/be knowledgeable",
            "be alive": "live/be alive",
            "man": "man/male",
            "meat": "meat/flesh",
            "near": "near/close",
            "open": "open/uncover",
            "person": "person/human being",
            "road": "road/path",
            "ashamed": "shy/ashamed",
            "smell (tr/intr)": "sniff/smell",
            "stab": "stab/pierce",
            "tie up": "tie up/fasten",
            "woman": "woman/female",
            "sick": "painful/sick",
        }

        args.writer.add_sources()
        language_lookup = args.writer.add_languages(
                id_factory=lambda l: slug(l["Name"], lowercase=False),
                lookup_factory='Name'
                )
        concept_lookup = args.writer.add_concepts(
                id_factory=lambda x: x.id.split('-')[-1]+'_'+slug(x.english),
                lookup_factory='Name'
                )
        for s, t in concept_replacements.items():
            concept_lookup[s] = concept_lookup[t]

        singletons = 1
        for entry in pb(
                self.raw_dir.read_csv(
                    "bowernpny.tsv", 
                    delimiter="\t",
                    quoting=QUOTE_NONE,
                    dicts=True)):
            if entry['Code'] == '0':
                cogid = singletons
                singletons += 1
            else:
                cogid = entry['Code']
            for form in args.writer.add_forms_from_value(
                Language_ID=language_lookup[entry['Language']],
                Parameter_ID=concept_lookup[entry['Gloss']],
                Value=entry['Form'],
                Source=["Bowern2012"]
            ):
                args.writer.add_cognate(
                    lexeme=form,
                    Cognateset_ID=cogid,
                    Source=['Bowern2012'],
                    )

    def cmd_download(self, args):
        self.raw_dir.download(
            "https://zenodo.org/record/1005671/files/Pny2012Codes.tsv",
            "bowernpny.tsv")
        self.raw_dir.write("sources.bib", getEvoBibAsBibtex("Bowern2012"))
