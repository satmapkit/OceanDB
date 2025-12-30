{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
   :show-inheritance:
   :members:
   :undoc-members:
   :member-order: groupwise

   .. autosummary::
      :toctree: generated

   .. =================== METHODS ===================
   {% block methods %}

   {% if methods %}
   .. rubric:: {{ _('Methods') }}

   .. autosummary::
   {% for item in methods %}
     {% if item in members and item not in inherited_members%}
        ~{{ name }}.{{ item }}
     {% endif %}
   {%- endfor %}
   {% endif %}
   {% endblock %}
   .. ===============================================

   .. ================= Attributes ==================
   {% block attributes %}
   {% if attributes %}
   .. rubric:: {{ _('Attributes') }}

   .. autosummary::
   {% for item in attributes %}
     {% if item in members and item not in inherited_members%}
        ~{{ name }}.{{ item }}
     {% endif %}
   {%- endfor %}
   {% endif %}
   {% endblock %}
   .. ===============================================

