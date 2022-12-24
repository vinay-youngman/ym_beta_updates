# -*- coding: utf-8 -*-

{
    'name' : 'Beta Hooks',
    'version' : '1.0.0',
    'category': 'Sales/CRM',
    'summary': 'For sending updates to customer from odoo to beta',
    'description': """For sending updates to customer from odoo to beta""",
    'author': "Vinay",
    'website': "https://www.youngman.co.in/",
    'sequence': -100,

    'depends': ['base'],

    'data': [
        'views/views.xml'
    ],

    'application': True,
    'installable': True,
    'auto_install': False,
}
