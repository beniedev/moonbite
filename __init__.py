"""Native Hermes directory-plugin entry point."""

if __package__:
    from .moonbite_plugin import register
else:
    # Pytest imports a repository-root ``__init__.py`` as a top-level module
    # when the checkout directory is not a valid Python package name.
    from moonbite_plugin import register

__all__ = ["register"]
