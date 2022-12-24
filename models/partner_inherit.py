# -*- coding: utf-8 -*-

from odoo import api, fields, models, _

import logging
import mysql.connector
from mysql.connector import Error

_logger = logging.getLogger(__name__)

from odoo.exceptions import UserError


class ResPartner(models.Model):
    _inherit = 'res.partner'

    @api.onchange('bill_submission_process')
    def _onchange_bill_submission_process(self):
        if self._is_existing_record():
            self._execute_single_update("UPDATE customer_masters SET bill_submission = %s WHERE crm_master_id = %s", (self.bill_submission_process.name, self.id.origin))

    @api.onchange('user_id')
    def _onchange_user_id(self):
        if self._is_existing_record() and self.user_id:
            self._execute_single_update("update customer_masters set account_manager = (select id from users where LOWER(email) = LOWER(%s)) where crm_account_id = %s", (self.user_id.email, self.id.origin))

    @api.onchange('account_receivable')
    def _onchange_account_receivable(self):
        if self._is_existing_record() and self.account_receivable:
            self._execute_single_update("update customer_masters set account_receivable = (select id from users where LOWER(email) = LOWER(%s)) where crm_account_id = %s", (self.account_receivable.email, self.id.origin))

    @api.onchange('rental_advance', 'rental_order', 'security_cheque')
    def _onchange_documents(self):
        if self._is_existing_record():
            args = {
                'rental_order': 1 if self.rental_order else 0,
                'rental_advance': 1 if self.rental_advance else 0,
                'security_cheque': 1 if self.security_cheque else 0,
                'crm_id': self.id.origin
            }

            self._execute_single_update("update customer_masters set rental_advance = %(rental_advance)s, rental_order =%(rental_order)s, security_cheque = %(security_cheque)s where crm_account_id = %(crm_id)s", args)

    @api.onchange('credit_limit', 'credit_rating')
    def _onchange_credit(self):
        if self._is_existing_record():
            self._execute_single_update("update customer_masters set credit_limit = %s, credit_rating = %s where crm_account_id = %s", (self.credit_limit, self.credit_rating, self.id.origin))

    @api.onchange('property_payment_term_id')
    def _onchange_property_payment_term_id(self):
        if self._is_existing_record():
            raise UserError(_("This action is currently not supported. Please perform this action in BETA"))

    def _is_existing_record(self):
        return self.in_beta

    def _get_connection(self):
        connection = False
        cursor = False
        try:
            beta_db_url = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db_url')
            beta_db_port = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db_port')
            beta_db = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db')
            beta_db_username = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db_username')
            beta_db_password = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db_password')

            if not (beta_db_url or beta_db_port or beta_db or beta_db_username  or beta_db_password):
                raise UserError("Beta Database is not configured. Please as system admins to configure it")

            connection = mysql.connector.connect(
                host=beta_db_url,
                port=beta_db_port,
                user=beta_db_username,
                password=beta_db_password,
                database=beta_db
            )
            return connection
        except Error as e:
            _logger.error("Error while connecting to MySQL using Connection pool ", e)
            raise e

    def _execute_single_update(self, statement, args):
        connection = False
        cursor = False
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(statement, args)
        except Error as e:
            raise UserError(_("Could not perform selection action: " + str(e)))
        except UserError as e:
            raise e
        except Exception as e:
            raise UserError(_("Could not perform selection action: " + str(e)))
        finally:
            if connection and connection.is_connected() and cursor:
                cursor.close()
