import inspect
import logging

log = logging.getLogger(__name__)


# ref https://stackoverflow.com/a/28075525
def ensure_class_bases_begin_with(namespace, class_name, base_class):
    """Ensure the named class's bases start with the base class,
    and remove any existing bases that are subclassed from the new class.

    :param namespace: The namespace containing the class name.
    :param class_name: The name of the class to alter.
    :param base_class: The type to be the first base class for the
        newly created type.
    :return: ``None``.

    Call this function after ensuring `base_class` is
    available, before using the class named by `class_name`.

    """
    existing_class = namespace[class_name]
    assert isinstance(existing_class, type)

    # bases = list(existing_class.__bases__)
    log.debug(f"new baseclass:  {base_class} {base_class.__name__} for class: {class_name}")
    log.debug(f"initial bases:  {existing_class.__bases__}")
    # Remove any superclasses that are subclassed from the new class
    bases = [
        base
        for base in existing_class.__bases__
        if not (
            issubclass(base, base_class)
            or (base.__name__ == base_class.__name__ and inspect.getmodule(base) is inspect.getmodule(base_class))
        )
    ]
    # bases = [base for base in bases if not #  repr() prints namesapes classname
    log.debug(f"trimmed bases:  {bases}")

    # TODO check this with removed superclasses
    # if base_class is bases[0]:
    #     # Already bound to a type with the right bases.
    #     return
    bases.insert(0, base_class)
    log.debug(f"final  bases:  {bases}")

    new_class_namespace = existing_class.__dict__.copy()
    # Type creation will assign the correct ‘__dict__’ attribute.
    new_class_namespace.pop('__dict__', None)

    metaclass = existing_class.__metaclass__
    new_class = metaclass(class_name, tuple(bases), new_class_namespace)

    log.debug(f"new_class bases:  {new_class.__bases__}")
    namespace[class_name] = new_class


def set_base_class(namespace, class_name, base_class):
    """Ensure the named class's base class is the new_base_class.

    :param namespace: The namespace containing the class name.
    :param class_name: The name of the class to alter.
    :param base_class: The type to be the base class for the
        newly created type.
    :return: ``None``.

    Call this function after ensuring `base_class` is
    available, before using the class named by `class_name`.

    """
    existing_class = namespace[class_name]
    assert isinstance(existing_class, type)

    new_class_namespace = existing_class.__dict__.copy()
    # Type creation will assign the correct ‘__dict__’ attribute.
    new_class_namespace.pop('__dict__', None)
    metaclass = existing_class.__metaclass__
    new_class = metaclass(class_name, tuple([base_class]), new_class_namespace)
    namespace[class_name] = new_class
