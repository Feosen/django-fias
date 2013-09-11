#coding: utf-8
from __future__ import unicode_literals, absolute_import

from django.forms.models import ModelChoiceField
from django.utils.text import force_unicode

from django_select2.fields import HeavyModelSelect2ChoiceField

from fias import widgets


class AddressSelect2Field(HeavyModelSelect2ChoiceField):

    widget = widgets.AddressSelect2

    def __init__(self, *args, **kwargs):
        super(AddressSelect2Field, self).__init__(*args, **kwargs)
        self.widget.field = self

    def _txt_for_val(self, value):
        if not value:
            return
        obj = self.queryset.get(pk=value)
        lst = [force_unicode(obj)]

        def make_list(o):
            if o.aolevel > 1:
                try:
                    parent = self.queryset.get(aoguid=o.parentguid)
                except self.queryset.model.DoesNotExist:
                    return
                else:
                    lst.append(force_unicode(parent))
                    make_list(parent)

        make_list(obj)
        return ', '.join(lst[::-1])


class ChainedAreaField(ModelChoiceField):

    def __init__(self, app_name, model_name, address_field, *args, **kwargs):

        defaults = {
            'widget': widgets.AreaChainedSelect(app_name, model_name, address_field)
        }
        defaults.update(kwargs)

        super(ChainedAreaField, self).__init__(*args, **defaults)