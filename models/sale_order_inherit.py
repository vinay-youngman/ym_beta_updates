# -*- coding: utf-8 -*-

from odoo import api, fields, models, _

import logging
import mysql.connector
from mysql.connector import Error
import json
import requests

import datetime, mimetypes

_logger = logging.getLogger(__name__)

from odoo.exceptions import UserError, ValidationError

from odoo.exceptions import UserError


def get_create_by(created_by_result):
    if not created_by_result or len(created_by_result) == 0:
        raise UserError("Your account does not exist in beta")
    else:
        return created_by_result[0]


def get_beta_customer_id(customer_id_result):
    if not customer_id_result or len(customer_id_result) == 0:
        raise UserError("This branch has not been created in beta")
    else:
        return customer_id_result[0]


def get_beta_godown_id(godown_result):
    if not godown_result or len(godown_result) == 0:
        raise UserError("Either of billing or parent godown is not present in beta")
    else:
        return godown_result[0]


def get_quotation_insert_query():
    return "INSERT INTO quotations (created_by, customer_id, contact_name, phone_number, site_name, price_type, total, freight, gstn, billing_address_line, billing_address_city, billing_address_pincode, delivery_address_line, delivery_address_city, delivery_address_pincode, delivery_date, pickup_date, security_amt, freight_payment, godown_id, crm_account_id, created_at, updated_at) VALUES (%(created_by)s, %(customer_id)s, %(contact_name)s, %(phone_number)s, %(site_name)s, %(price_type)s, %(total)s, %(freight)s, %(gstn)s, %(billing_address_line)s, %(billing_address_city)s, %(billing_address_pincode)s, %(delivery_address_line)s, %(delivery_address_city)s, %(delivery_address_pincode)s, %(delivery_date)s, %(pickup_date)s, %(security_amt)s, %(freight_payment)s, %(godown_id)s, %(crm_account_id)s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"


def get_quotation_items_insert_query():
    return "insert into quotation_items (quotation_id, item_code, unit_price, quantity, created_at, updated_at) VALUES (%(quotation_id)s, %(item_code)s, %(unit_price)s, %(quantity)s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"


def get_beta_user_id_from_email_query():
    return "select id from users where LOWER(email) = %s"


def get_beta_customer_id_from_gstn():
    return "select id from customers where UPPER(gstn) = %s"


def get_location_insert_query():
    return "INSERT INTO locations (location_name, type, state_code) VALUES (%s, 'job_order', %s)"


def get_state_code_from_state_alpha_query(state_code):
    return "SELECT state_code FROM states WHERE state_alpha = '{}'".format(state_code)


def get_order_insert_query():
    return "INSERT INTO orders (quotation_id, customer_id, job_order, po_no, place_of_supply, gstn, security_cheque, rental_advance, rental_order, godown_id, freight_amount, billing_godown, created_by, total, is_authorized, created_at, updated_at) " \
           "VALUES (%(quotation_id)s, %(customer_id)s, %(job_order)s, %(po_no)s, %(place_of_supply)s, %(gstn)s, %(security_cheque)s, %(rental_advance)s, %(rental_order)s, %(godown_id)s, %(freight_amount)s, %(billing_godown)s, %(created_by)s, %(total)s, %(is_authorized)s,CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"


def get_beta_godown_id_by_name_query(godown_name):
    return "SELECT id from locations where type='godown' and location_name = '{}'".format(godown_name)


def get_update_quotation_with_order_query():
    return "UPDATE quotations set order_id = %s, converted_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP where id = %s"


def get_order_po_insert_query():
    return "INSERT INTO order_po(order_id, po_no, po_amt, balance) VALUES (%s, %s, %s, %s)"


def get_order_po_details_insert_query():
    return "INSERT INTO order_po_details(order_id, po_no, po_date , item_code , quantity) VALUES (%(order_id)s,%(po_no)s,%(po_date)s,%(item_code)s,%(quantity)s)"


def get_order_item_feed_insert_query():
    return "INSERT INTO order_item_feed(job_order, item_code, quantity) VALUES (%(job_order)s, %(item_code)s, %(quantity)s)"


def get_billing_process_insert_query():
    return "insert into billing_process (order_id, odoo_site_contact, odoo_office_contact, bill_submission_location, site_address, site_pincode, office_address, office_pincode, process) " \
           "VALUES (%(order_id)s, %(odoo_site_contact)s,%(odoo_office_contact)s,%(bill_submission_location)s,%(site_address)s,%(site_pincode)s,%(office_address)s,%(office_pincode)s,%(process)s)"


def _concatenate_address_string(address_strings):
    arr = [x for x in address_strings if x]
    return ', '.join(map(str, arr))


def _get_order_item_feed_details(job_order, quotation_items):
    item_feed_details = []
    for item in quotation_items:
        item_feed_details.append({
            'job_order': job_order,
            'item_code': item['item_code'],
            'quantity': item['quantity'],
        })
    return item_feed_details

def _get_beta_compatible_freight_type(freight_type):
    frieght_map = {
        'freight_type1': 'It has been agreed 1st Dispatch and final Pickup will be done by Youngman',
        'freight_type2': 'It has been agreed 1st Dispatch will be done by Youngman and final Pickup will be done by Customer on his cost',
        'freight_type3': 'It has been agreed 1st Dispatch will be done by Customer on his cost and final Pickup would be done by Youngman',
        'freight_type4': 'It has been agreed 1st Dispatch will be done by Customer on his cost and final Pickup is already paid by Customer',
        'freight_type5': 'It has been agreed 1st Dispatch and final Pickup will be done by Customer on his cost'
    }

    return frieght_map.get(freight_type)

class SaleOrderInherit(models.Model):
    _inherit = 'sale.order'

    def action_confirm(self):
        self._validate_order_before_confirming()
        self._create_customer_in_beta_if_not_exists()
        self._create_branch_in_beta_if_not_exists() #For branches that were added post initial customer creation

        try:
            connection = self._get_connection()
            connection.autocommit = False
            cursor = connection.cursor()
            email = self.env.user.login.lower()

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Get created by from beta")
            cursor.execute(get_beta_user_id_from_email_query(), [email])
            created_by = get_create_by(cursor.fetchone())

            if self.partner_id.team_id.name == 'INSIDE SALES':
                created_by = 568

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Get customer id from beta")
            cursor.execute(get_beta_customer_id_from_gstn(), [self.customer_branch.gstn])
            customer_id = get_beta_customer_id(cursor.fetchone())

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Get billing godown id from beta")
            cursor.execute(get_beta_godown_id_by_name_query(self.bill_godown.name))
            beta_bill_godown_id = get_beta_godown_id(cursor.fetchone())

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Get parent godown id from beta")
            cursor.execute(get_beta_godown_id_by_name_query(self.godown.name))
            beta_godown_id = get_beta_godown_id(cursor.fetchone())

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Trying to save quotation")
            quotation_total = self._get_quotation_total()
            quotation = self._get_quotation_data(created_by, customer_id, beta_godown_id, quotation_total)
            cursor.execute(get_quotation_insert_query(), quotation)
            quotation_id = cursor.lastrowid
            _logger.info("evt=SEND_ORDER_TO_BETA msg=Quotation saved with id" + str(quotation_id))

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Trying to save quoataion items")
            quotation_items = self._get_quotation_items_and_total(quotation_id)
            cursor.executemany(get_quotation_items_insert_query(), quotation_items)
            _logger.info("evt=SEND_ORDER_TO_BETA msg=Quotation items saved")

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Generating job order number")
            job_order_number = self._generate_job_number(created_by, customer_id, quotation_id)
            self.job_order = job_order_number
            self.name = job_order_number

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Get Place of supply code from beta")
            cursor.execute(get_state_code_from_state_alpha_query(self.place_of_supply.code))

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Trying to create job order location")
            place_of_supply_code = cursor.fetchone()[0]
            cursor.execute(get_location_insert_query(), (job_order_number, place_of_supply_code))
            location_id = cursor.lastrowid
            _logger.info("evt=SEND_ORDER_TO_BETA msg=Location created with id" + str(location_id))

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Trying to create order")
            order_data = self._get_order_data(created_by, customer_id, quotation_id, quotation_total, job_order_number,
                                              place_of_supply_code, beta_bill_godown_id, beta_godown_id, "0")
            cursor.execute(get_order_insert_query(), order_data)
            order_id = cursor.lastrowid
            _logger.info("evt=SEND_ORDER_TO_BETA msg=Order saved with id" + str(order_id))

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Saving PO data")
            cursor.execute(get_order_po_insert_query(), (order_id, self.po_number, self.po_amount, self.po_amount))
            po_details = self._generate_po_details(order_id, quotation_items)

            _logger.info("evt=SEND_ORDER_TO_BETA msg=Saving PO details")
            cursor.executemany(get_order_po_details_insert_query(), po_details)

            billing_process_data = self._get_billing_process_data(order_id, location_id)
            query = get_billing_process_insert_query()
            _logger.info(
                "evt=SEND_ORDER_TO_BETA msg=Saving Bill Submission details " + str(billing_process_data) + " query=")
            cursor.execute(query, billing_process_data)

            if self._is_to_be_auto_approved():
                _logger.info("evt=SEND_ORDER_TO_BETA msg=Updating quotation with order id and authorising it")
                cursor.execute(get_update_quotation_with_order_query(), (order_id, quotation_id))
                cursor.execute("UPDATE orders SET is_authorized = 1 WHERE id =" + str(order_id))

                _logger.info("evt=SEND_ORDER_TO_BETA msg=insert into order item feed")
                cursor.executemany(get_order_item_feed_insert_query(),
                                   _get_order_item_feed_details(job_order_number, quotation_items))

            super(SaleOrderInherit, self).action_confirm()
            cursor.close()
            connection.commit()

        except Error as e:
            _logger.error("evt=SEND_ORDER_TO_BETA msg=", exc_info=1)
            raise e

    def _create_customer_in_beta_if_not_exists(self):
        try:
            master_customer = self.partner_id
            if not master_customer.in_beta:
                user_id = master_customer.user_id.login
                if master_customer.team_id.name == 'INSIDE SALES':
                    user_id = "customercare@youngman.co.in"

                branches = []
                for branch in master_customer.branch_ids:
                    branch_data = self._get_branch_data_for_saving_in_beta(branch, user_id)
                    branches.append(branch_data)


                payment_terms = master_customer.property_payment_term_id.name if master_customer.property_payment_term_id else None

                if not payment_terms or 'Immediate' in payment_terms:
                    due_days = 0
                elif '2 Months' in payment_terms:
                    due_days = 60
                else:
                    due_days = [int(i) for i in payment_terms.split() if i.isdigit()][0]

                payload = json.dumps({
                    "partner": [
                        {
                            "id": master_customer.id,
                            "name": master_customer.name,
                            "vat": master_customer.vat,
                            "email": master_customer.email,
                            "phone": master_customer.phone,
                            "street": master_customer.street,
                            "street2": master_customer.street2,
                            "city": master_customer.city,
                            "zip": master_customer.zip,
                            "business_type": master_customer.business_type.name,
                            "is_company": master_customer.is_company,
                            "rental_advance": master_customer.rental_advance,
                            "rental_order": master_customer.rental_order,
                            "security_cheque": master_customer.security_cheque,
                            "user_id": user_id,
                            "account_receivable": master_customer.account_receivable.email,
                            "credit_limit": master_customer.credit_limit,
                            "credit_rating": master_customer.credit_rating,
                            "days_due": False if not due_days else str(due_days),
                            "billing_process": master_customer.bill_submission_process.name

                        }
                    ],
                    "branches": branches
                })

                beta_customer_save_endpoint = self._get_customer_creation_endpoint()

                response = requests.request("POST", beta_customer_save_endpoint, headers={'Content-Type': 'application/json'}, data=payload, verify=False)

                if not response.ok:
                    raise UserError(_("Unable to save customer in beta."))
                else:
                    master_customer.in_beta = True
                    for branch in master_customer.branch_ids:
                        branch.in_beta = True

        except requests.exceptions.HTTPError as errh:
            raise UserError("Http Error:" + _(errh))
        except requests.exceptions.ConnectionError as errc:
            raise UserError("Error Connecting:" + _(errc))
        except requests.exceptions.Timeout as errt:
            raise UserError("Timeout Error:" + _(errt))
        except requests.exceptions.RequestException as err:
            raise UserError("OOps:" + _(err))
        except Error as e:
            raise UserError(_(e))

    def _get_customer_creation_endpoint(self):
        beta_customer_save_endpoint = self.env['ir.config_parameter'].sudo().get_param(
            'ym_beta_updates.beta_customer_save_endpoint')
        if not beta_customer_save_endpoint:
            raise UserError(_("Beta save customer endpoint is not configured. Please reach out to system admins."))
        return beta_customer_save_endpoint

    def _get_branch_data_for_saving_in_beta(self, branch, user_id):
        branch_data = {
            "odoo_branch_id": branch.id,
            "branch_name": branch.name,
            "gstn": branch.gstn,
            "email": branch.email,
            "phone": branch.phone,
            "mobile": branch.mobile,
            "user_id": user_id,
            "bde": branch.bde.email,
            "branch_contact_name": branch.branch_contact_name,
            "billing_address_line": _concatenate_address_string(
                [branch.street, branch.street2, branch.state_id.name if branch.state_id else False]),
            "billing_address_city": branch.city,
            "billing_address_pincode": branch.zip,
            "mailing_address_line": _concatenate_address_string([branch.mailing_street, branch.mailing_street2,
                                                                 branch.mailing_state_id.name if branch.mailing_state_id else False]),
            "mailing_address_city": branch.mailing_city,
            "mailing_address_pincode": branch.mailing_zip
        }
        return branch_data

    def _create_branch_in_beta_if_not_exists(self):
        try:
            if not self.customer_branch.in_beta:
                user_id = self.parent_id.user_id.login
                branch_data = self._get_branch_data_for_saving_in_beta(self.customer_branch, user_id)

                beta_branch_save_endpoint = self._get_branch_creation_endpoint()

                response = requests.request("POST", beta_branch_save_endpoint, headers={'Content-Type': 'application/json'}, data=branch_data, verify=False)

                if not response.ok:
                    raise UserError(_("Unable to save customer branch in beta."))
                else:
                    self.customer_branch.in_beta = True
        except requests.exceptions.HTTPError as errh:
            raise UserError("Http Error:" + _(errh))
        except requests.exceptions.ConnectionError as errc:
            raise UserError("Error Connecting:" + _(errc))
        except requests.exceptions.Timeout as errt:
            raise UserError("Timeout Error:" + _(errt))
        except requests.exceptions.RequestException as err:
            raise UserError("OOps:" + _(err))
        except Error as e:
            raise UserError(_(e))

    def _get_branch_creation_endpoint(self):
        beta_branch_save_endpoint = self.env['ir.config_parameter'].sudo().get_param(
            'ym_beta_updates.beta_branch_save_endpoint')
        if not beta_branch_save_endpoint:
            raise UserError(
                _("Beta save customer branch endpoint is not configured. Please reach out to system admins."))
        return beta_branch_save_endpoint

    def _is_to_be_auto_approved(self):
        if self.partner_id.team_id.name == "PAM" and self.partner_id.credit_rating in ['A', 'B']:
            _logger.info("evt=SEND_ORDER_TO_BETA msg=Order will be auto approved")
            return True
        return False

    def _get_billing_process_data(self, order_id, location_id):
        billing_process_data = {
            'order_id': order_id,
            'odoo_site_contact': self.bill_site_contact.id,
            'odoo_office_contact': self.bill_office_contact.id,
            'bill_submission_location': location_id,
            'site_address': _concatenate_address_string([
                self.delivery_street,
                self.delivery_street2,
                self.delivery_city,
                self.delivery_state_id.name if self.delivery_state_id else False]),
            'site_pincode': self.delivery_zip,
            'office_address': _concatenate_address_string([
                self.bill_submission_office_branch.street,
                self.bill_submission_office_branch.street2,
                self.bill_submission_office_branch.city,
                self.bill_submission_office_branch.state_id.name if self.bill_submission_office_branch.state_id else False]),
            'office_pincode': self.bill_submission_office_branch.zip,
            'process': self.partner_id.bill_submission_process.name
        }
        return billing_process_data

    def _generate_po_details(self, order_id, quotation_items):
        po_details = []
        for item in quotation_items:
            po_details.append({
                'order_id': order_id,
                'po_no': self.po_number,
                'po_date': self.po_date.strftime('%Y-%m-%d'),
                'po_amount': self.po_amount,
                'item_code': item['item_code'],
                'quantity': item['quantity'],
            })
        return po_details

    def _get_quotation_total(self):
        quotation_total = 0
        for order_line in self.order_line:
            quotation_total = quotation_total + (order_line.price_unit * order_line.product_uom_qty)
        return quotation_total

    def _get_quotation_items_and_total(self, quotation_id):
        quotation_items = []
        for order_line in self.order_line:
            quotation_items.append({
                'quotation_id': quotation_id,
                'item_code': order_line.product_id.default_code,
                'unit_price': order_line.price_unit,
                'quantity': order_line.product_uom_qty
            })
        return quotation_items

    def _validate_order_before_confirming(self):
        if self.tentative_quo:
            raise ValidationError(_("Confirmation of tentative quotation is not allowed"))
        if not self.po_number:
            raise ValidationError(_('PO Number is mandatory for confirming a quotation'))
        if not self.po_amount:
            raise ValidationError(_('PO Amount is mandatory for confirming a quotation'))
        if not self.po_date:
            raise ValidationError(_('PO Date is mandatory for confirming a quotation'))
        if not self.place_of_supply:
            raise ValidationError(_('Place of Supply is mandatory for confirming a quotation'))
        if not self.rental_order and self.customer_branch.rental_order is True:
            raise ValidationError(_('Rental Order is mandatory for this customer'))
        if not self.rental_advance and self.customer_branch.rental_advance is True:
            raise ValidationError(_('Rental Advance is mandatory for this customer'))
        if not self.security_cheque and self.customer_branch.security_cheque is True:
            raise ValidationError(_('Security Cheque is mandatory for this customer'))
        if not self.partner_id.vat:
            raise ValidationError(_("This customer does not have a PAN. Please check customer details"))
        if not self.partner_id.bill_submission_process:
            raise ValidationError(
                _("This customer does not have a Bill submission process defined. Please check customer details"))
        if self.partner_id.bill_submission_process.code == 'email' and not self.bill_submission_email:
            raise ValidationError(_("Bill submission email is required."))
        if self.partner_id.bill_submission_process.code in ['site',
                                                            'site_office'] and not self.site_bill_submission_godown:
            raise ValidationError(_("Site Bill submission godown is required."))
        if self.partner_id.bill_submission_process.code in ['site',
                                                            'site_office'] and not self.office_bill_submission_godown:
            raise ValidationError(_("Office Bill submission godown is required."))
        if self.partner_id.bill_submission_process.code in ['site', 'site_office'] and not self.bill_site_contact:
            raise ValidationError(_("Bill Site Contact is required."))
        if self.partner_id.bill_submission_process.code in ['office', 'site_office'] and not self.bill_office_contact:
            raise ValidationError(_("Customer Bill Submission Office Contac"))
        if self.partner_id.bill_submission_process.code in ['office',
                                                            'site_office'] and not self.bill_submission_office_branch:
            raise ValidationError(_("Bill Submission Office Branch is required."))
        if not self.partner_id.team_id:
            raise ValidationError(_("Sales team for master customer is not available."))
        if not self.partner_id.credit_rating:
            raise ValidationError(_("Credit Rating for master customer is not available."))

    def _generate_job_number(self, created_by, customer_id, quotation_id):
        today = datetime.date.today()
        job_order_number = str(today.year) + "/" + today.strftime("%b") + "/" + self.jobsite_id.name + "/" + str(
            created_by) + "/" + str(customer_id) + "/" + self.po_number + "/" + str(quotation_id)
        return job_order_number

    def _get_order_data(self, created_by, customer_id, quotation_id, quotation_total, job_order_number,
                        place_of_supply_code, beta_bill_godown_id, beta_godown_id, is_authorized):
        return {
            'quotation_id': quotation_id,
            'customer_id': customer_id,
            'job_order': job_order_number,
            'po_no': self.po_number,
            'place_of_supply': place_of_supply_code,
            'gstn': self.customer_branch.gstn,
            'security_cheque': self._get_document_if_exists('security_cheque'),
            'rental_advance': self._get_document_if_exists('rental_advance'),
            'rental_order': self._get_document_if_exists('rental_order'),
            'godown_id': beta_godown_id,
            'freight_amount': self.freight_amount,
            'billing_godown': beta_bill_godown_id,
            'created_by': created_by,
            'total': quotation_total,
            'is_authorized': is_authorized

        }

    def _get_document_if_exists(self, field_name):
        PREFIX = "s3://"
        attachment = self.env['ir.attachment'].sudo().search(
            [('res_model', '=', 'sale.order'), ('res_field', '=', field_name), ('res_id', '=', self.id)])

        fname = attachment.store_fname if attachment else ""
        mimetype = attachment.mimetype if attachment else ""

        extension = mimetypes.guess_extension(mimetype, strict=True)

        if fname.startswith(PREFIX):
            return fname[len(PREFIX):] + extension if extension else ""

        return None

    def _get_quotation_data(self, created_by, customer_id, beta_godown_id, quotation_total):
        return {
            'created_by': created_by,
            'customer_id': customer_id,
            'contact_name': self.purchaser_name.name,
            'phone_number': self.purchaser_name.mobile,
            'site_name': self.jobsite_id.name,
            'price_type': self.price_type,
            'total': quotation_total,
            'freight': self.freight_amount,
            'gstn': 'Gstn Moved to Order',
            'billing_address_line': _concatenate_address_string(
                [self.billing_street, self.billing_street2, self.billing_city]),
            'billing_address_city': self.billing_state_id.name if self.billing_state_id else "",
            'billing_address_pincode': self.billing_zip,
            'delivery_address_line': _concatenate_address_string(
                [self.delivery_street, self.delivery_street2, self.delivery_city]),
            'delivery_address_city': self.delivery_state_id.name if self.delivery_state_id else "",
            'delivery_address_pincode': self.delivery_zip,
            'delivery_date': self.delivery_date.strftime('%Y-%m-%d'),
            'pickup_date': self.pickup_date.strftime('%Y-%m-%d'),
            'security_amt': self.security_amount if self.security_amount else 0.0,
            'freight_payment': _get_beta_compatible_freight_type(self.freight_paid_by),
            'godown_id': beta_godown_id,
            'sign_type': 'MANUAL',
            'crm_account_id': self.id,
        }

    def _get_connection(self):
        connection = False
        cursor = False
        try:
            beta_db_url = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db_url')
            beta_db_port = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db_port')
            beta_db = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db')
            beta_db_username = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db_username')
            beta_db_password = self.env['ir.config_parameter'].sudo().get_param('ym_beta_updates.beta_db_password')

            if not (beta_db_url or beta_db_port or beta_db or beta_db_username or beta_db_password):
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
