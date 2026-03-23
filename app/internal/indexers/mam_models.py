import json

from pydantic import BaseModel


class _Result(BaseModel):
    id: int
    author_info: str | None = None
    narrator_info: str | None = None
    series_info: str | None = None
    language_info: str | None = None
    tags: str | None = None
    personal_freeleech: int
    free: int
    fl_vip: int
    vip: int
    filetype: str | None = None
    synopsis: str | None = None
    cover_image: str | None = None
    book_title: str | None = None
    title: str | None = None
    synopsis_image: str | None = None
    category: str | int | None = None
    added: str | None = None

    @property
    def display_title(self) -> str:
        return self.book_title or self.title or f"MAM-{self.id}"

    def _parse_dict_values(self, value: str | None) -> list[str]:
        if not value:
            return []
        content = json.loads(value)  # pyright: ignore[reportAny]
        if isinstance(content, dict):
            return [
                x for x in content.values() if isinstance(x, str)
            ]  # pyright: ignore[reportUnknownVariableType]
        return []

    @property
    def authors(self) -> list[str]:
        """Response type of authors and narrators is a stringified json object"""
        return self._parse_dict_values(self.author_info)

    @property
    def narrators(self) -> list[str]:
        return self._parse_dict_values(self.narrator_info)

    @property
    def series(self) -> list[str]:
        if not self.series_info:
            return []
        try:
            content = json.loads(self.series_info)
            if isinstance(content, dict):
                series_list = []
                for val in content.values():
                    if isinstance(val, list) and len(val) >= 2:
                        name, num = val[0], val[1]
                        series_list.append(f"{name} #{num}")
                    elif isinstance(val, str):
                        series_list.append(val)
                return series_list
        except Exception:
            pass
        return []

    @property
    def languages(self) -> list[str]:
        return self._parse_dict_values(self.language_info)


class _MamResponse(BaseModel):
    data: list[_Result]
