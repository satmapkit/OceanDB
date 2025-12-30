{{ fullname | escape | underline}}

.. automodule:: {{ fullname }}

   {% block classes %}
   {% if classes %}
   .. rubric:: Classes

   .. autosummary::
      :toctree:
      :template: class-template.rst
   {% for item in classes %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

