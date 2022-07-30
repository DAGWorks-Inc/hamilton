import dataclasses
import functools
import logging
from typing import Callable, Optional, Union, Tuple

from hamilton import version

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Version:
    major: int
    minor: int
    patch: int

    def __gt__(self, other: 'Version'):
        return (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)

    @staticmethod
    def current() -> 'Version':
        current_version = version.VERSION
        if len(current_version) > 3:  # This means we have an RC
            current_version = current_version[0:3]  # Then let's ignore it
        return Version(*current_version)  # TODO, add some validation

    def __repr__(self):
        return '.'.join(map(str, [self.major, self.minor, self.patch]))


CURRENT_VERSION = Version.current()


class DeprecationError(Exception):
    def raise_(self):
        raise self


@dataclasses.dataclass
class deprecated:
    """Deprecation decorator -- use judiciously! For example:
    @deprecate(
        warn_starting=(1,10,0)
        fail_starting=(2,0,0),
        use_instead=parameterize_values,
        reason='We have redefined the parameterization decorators to consist of `parametrize`, `parametrize_inputs`, and `parametrize_values`
        migration_guide="https://github.com/stitchfix/hamilton/..."
    )
    class parameterized(...):
       ...
    Note this locks into a future contract (although it *can* be changed), so if you promise to deprecate something by X.0, then do it!

    """
    warn_starting: Union[Tuple[int, int, int], Version]
    fail_starting: Union[Tuple[int, int, int], Version]
    use_this: Optional[Callable]  # If this is None, it means this functionality is no longer supported.
    explanation: str
    migration_guide: Optional[str]  # If this is None, this means that the use_instead is a drop in replacement
    current_version: Union[Tuple[int, int, int], Version] = dataclasses.field(default=CURRENT_VERSION)
    warn_action: Callable[[str], None] = dataclasses.field(default=logger.warning)
    fail_action: Callable[[str], None] = dataclasses.field(default=lambda message: DeprecationError(message).raise_())

    @staticmethod
    def _raise_failure(message: str):
        raise DeprecationError(message)

    @staticmethod
    def ensure_version_type(version_spec: Union[Tuple[int, int, int], Version]) -> Version:
        if isinstance(version_spec, tuple):
            return Version(*version_spec)
        return version_spec

    def __post_init__(self):
        if self.use_this is None:
            if self.migration_guide is None:
                raise ValueError('@deprecate must include a migration guide if there is no replacement.')
        self.warn_starting = deprecated.ensure_version_type(self.warn_starting)
        self.fail_starting = deprecated.ensure_version_type(self.fail_starting)
        self.current_version = deprecated.ensure_version_type(self.current_version)
        self._validate_fail_starting()

    def _validate_fail_starting(self):
        if self.fail_starting.major > 0:  # This means we're past alpha. We are, but nice to have...
            if self.fail_starting.minor != 0 or self.fail_starting.patch != 0:
                raise ValueError(f'Can only deprecate starting on major version releases. {self.fail_starting} is not valid.')
        if self.warn_starting > self.fail_starting:
            raise ValueError(f'warn_starting must come before fail_starting. {self.fail_starting} < {self.warn_starting}')

    def _do_action(self, fn: Callable):
        if self._should_fail():
            failure_message = ' '.join(
                [
                    f'{fn.__qualname__} has been deprecated, as of hamilton version: {self.fail_starting}.',
                    f'{self.explanation}'
                ] + \
                ([f'Instead, you should be using: {self.use_this.__qualname__}.'] if self.use_this is not None else []) + \
                ([f'For migration, see: {self.migration_guide}.'] if self.migration_guide is not None else [f'This is a drop-in replacement.']))
            self.fail_action(failure_message)
        elif self._should_warn():
            warn_message = ' '.join(
                [
                    f'{fn.__qualname__} will be deprecated by of hamilton version: {self.fail_starting}.',
                    f'{self.explanation}'
                ] + \
                ([f'Instead, you should be using: {self.use_this.__qualname__}.'] if self.use_this is not None else []) + \
                ([f'For migration, see: {self.migration_guide}.'] if self.migration_guide is not None else [f'This is a drop-in replacement.']))
            self.warn_action(warn_message)

    def _should_warn(self) -> bool:
        return self.current_version > self.warn_starting

    def _should_fail(self) -> bool:
        return self.current_version > self.fail_starting

    def __call__(self, fn: Callable):
        self._do_action(fn)
        return fn
