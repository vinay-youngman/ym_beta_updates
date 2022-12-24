# -*- coding: utf-8 -*-

from odoo import models, fields, api


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'
    beta_db_url = fields.Char(string='Beta DB Url', config_parameter='ym_beta_updates.beta_db_url')
    beta_db_port = fields.Char(string='Beta DB Port', config_parameter='ym_beta_updates.beta_db_port')
    beta_db = fields.Char(string='Beta DB Name', config_parameter='ym_beta_updates.beta_db')
    beta_db_username = fields.Char(string='Beta DB Username', config_parameter='ym_beta_updates.beta_db_username')
    beta_db_password = fields.Char(string='Beta DB Password', config_parameter='ym_beta_updates.beta_db_password')


