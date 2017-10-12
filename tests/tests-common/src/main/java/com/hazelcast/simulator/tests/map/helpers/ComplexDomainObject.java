/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map.helpers;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

import static java.lang.String.format;

@SuppressWarnings({"unused", "checkstyle:explicitinitialization", "checkstyle:methodcount", "checkstyle:methodlength",
        "checkstyle:methodname", "checkstyle:membername", "checkstyle:parametername"})
public class ComplexDomainObject implements Portable {

    public static final int PORTABLE_FACTORY_ID = 10000002;
    public static final int PORTABLE_CLASS_ID = 1;

    public int UUID = 1;

    public long locality_id = -1L;
    public String locality_name = "";
    public long division_id = -1L;
    public String division_name = "";
    public long brand_id = -1L;
    public String brand_name = "";
    public long media_id = -1L;
    public String media_name = "";
    public int media_content_approval_instruction = 0;
    public int media_qc_approval_instruction = 0;
    public int media_clock_approval_instruction = 0;
    public int media_class_approval_instruction = 0;
    public int media_instruction_approval_instruction = 0;
    public int media_house_nr_instruction = 0;
    public boolean media_music_report_required = false;
    public int media_delivery_hours = 0;
    public long media_destination_id = -1L;
    public long media_slave_destination_id = -1L;

    public long copy_code_id = -1L;
    public long region_id = -1L;
    public long media_type_media_extension_id = -1L;
    public String copy_code = "";
    public boolean aired = false;
    public long poster_id = -1L;
    public int film_length = 0;
    public long first_air_date = -1;
    public int content_type = 0;
    public String title = "";
    public boolean stopped = false;
    public boolean request_upload = false;
    public long urr_time = -1;

    public long sponsorship_locality_id = -1L;
    public long sponsorship_division_id = -1L;
    public long music_report_request_time = -1;
    public int music_report_request_count = 0;
    public int music_to_report = 0;
    public boolean music_report_completed = false;
    public long music_report_completed_time = -1;
    public boolean music_report_completed_action = false;
    public String media_agency_planning = "";
    public String media_agency_buying = "";
    public String creative_agency = "";
    public String production_agency = "";
    public String post_production_agency = "";
    public String other_agency = "";
    public boolean disabled_for_upload = false;

    public String media_house_nr = "";
    public String b_p_code = "";
    public boolean created_from_mismatch = false;
    public boolean delivered_from_elsewhere = false;
    public long media_first_air_date = -1;
    public long delivery_deadline = -1;
    public long delivery_deadline_ms_algo = -1;
    public boolean inactive = false;

    public long copy_envelope_id = -1L;
    public int copy_envelope_action_state = 0;
    public long expiration_time = -1;
    public long copy_envelope_sender_id = -1L;
    public long approval_advert_user_id = -1L;
    public long approval_advert_time = -1;
    public int approval_advert_state = 0;
    public int quality_check_state = 0;
    public int proxy_creation_state = 0;
    public long qc_proxy_approval_time = -1;
    public boolean transcoding = false;

    public long delivery_id = -1L;
    public int delivery_action_state = 0;
    public long time_delivered = -1;
    public long slave_time_delivered = -1;
    public long pre_delivered_time = -1;
    public long slave_pre_delivered_time = -1;
    public long post_delivered_time = -1;
    public long slave_post_delivered_time = -1;

    public long content_approval_media_user_id = -1L;
    public long content_approval_media_time = -1;
    public int content_approval_media_state = 0;

    public long qc_approval_media_user_id = -1L;
    public long qc_approval_media_time = -1;
    public int qc_approval_media_state = 0;

    public long clock_approval_media_user_id = -1L;
    public long clock_approval_media_time = -1;
    public int clock_approval_media_state = 0;

    public long class_approval_media_user_id = -1L;
    public long class_approval_media_time = -1;
    public int class_approval_media_state = 0;
    public int num_instruction_approved = 0;
    public int num_instruction_rejected = 0;
    public int num_instruction_approval_not_performed = 0;
    public long download_speed = -1;
    public int mismatch = 0;
    public boolean mismatch_ignored = false;
    public int invoice_account_demand = 0;

    public ComplexDomainObject() {
    }

    @SuppressWarnings({"checkstyle:executablestatementcount", "checkstyle:parameternumber"})
    public ComplexDomainObject(int UUID, long locality_id, String locality_name,
                               long division_id, String division_name, long brand_id,
                               String brand_name, long media_id, String media_name,
                               int media_content_approval_instruction,
                               int media_qc_approval_instruction,
                               int media_clock_approval_instruction,
                               int media_class_approval_instruction,
                               int media_instruction_approval_instruction,
                               int media_house_nr_instruction,
                               boolean media_music_report_required, int media_delivery_hours,
                               long media_destination_id, long media_slave_destination_id,
                               long copy_code_id, long region_id,
                               long media_type_media_extension_id, String copy_code,
                               boolean aired, long poster_id, int film_length,
                               long first_air_date, int content_type, String title,
                               boolean stopped, boolean request_upload, long urr_time,
                               long sponsorship_locality_id, long sponsorship_division_id,
                               long music_report_request_time, int music_report_request_count,
                               int music_to_report, boolean music_report_completed,
                               long music_report_completed_time,
                               boolean music_report_completed_action,
                               String media_agency_planning, String media_agency_buying,
                               String creative_agency, String production_agency,
                               String post_production_agency, String other_agency,
                               boolean disabled_for_upload, String media_house_nr,
                               String b_p_code, boolean created_from_mismatch,
                               boolean delivered_from_elsewhere, long media_first_air_date,
                               long delivery_deadline, long delivery_deadline_ms_algo,
                               boolean inactive, long copy_envelope_id,
                               int copy_envelope_action_state, long expiration_time,
                               long copy_envelope_sender_id, long approval_advert_user_id,
                               long approval_advert_time, int approval_advert_state,
                               int quality_check_state, int proxy_creation_state,
                               long qc_proxy_approval_time, boolean transcoding,
                               long delivery_id, int delivery_action_state,
                               long time_delivered, long slave_time_delivered,
                               long pre_delivered_time, long slave_pre_delivered_time,
                               long post_delivered_time, long slave_post_delivered_time,
                               long content_approval_media_user_id,
                               long content_approval_media_time,
                               int content_approval_media_state,
                               long qc_approval_media_user_id, long qc_approval_media_time,
                               int qc_approval_media_state, long clock_approval_media_user_id,
                               long clock_approval_media_time, int clock_approval_media_state,
                               long class_approval_media_user_id,
                               long class_approval_media_time, int class_approval_media_state,
                               int num_instruction_approved, int num_instruction_rejected,
                               int num_instruction_approval_not_performed,
                               long download_speed, int mismatch, boolean mismatch_ignored,
                               int invoice_account_demand) {
        this.UUID = UUID;
        this.locality_id = locality_id;
        this.locality_name = locality_name;
        this.division_id = division_id;
        this.division_name = division_name;
        this.brand_id = brand_id;
        this.brand_name = brand_name;
        this.media_id = media_id;
        this.media_name = media_name;
        this.media_content_approval_instruction = media_content_approval_instruction;
        this.media_qc_approval_instruction = media_qc_approval_instruction;
        this.media_clock_approval_instruction = media_clock_approval_instruction;
        this.media_class_approval_instruction = media_class_approval_instruction;
        this.media_instruction_approval_instruction = media_instruction_approval_instruction;
        this.media_house_nr_instruction = media_house_nr_instruction;
        this.media_music_report_required = media_music_report_required;
        this.media_delivery_hours = media_delivery_hours;
        this.media_destination_id = media_destination_id;
        this.media_slave_destination_id = media_slave_destination_id;
        this.copy_code_id = copy_code_id;
        this.region_id = region_id;
        this.media_type_media_extension_id = media_type_media_extension_id;
        this.copy_code = copy_code;
        this.aired = aired;
        this.poster_id = poster_id;
        this.film_length = film_length;
        this.first_air_date = first_air_date;
        this.content_type = content_type;
        this.title = title;
        this.stopped = stopped;
        this.request_upload = request_upload;
        this.urr_time = urr_time;
        this.sponsorship_locality_id = sponsorship_locality_id;
        this.sponsorship_division_id = sponsorship_division_id;
        this.music_report_request_time = music_report_request_time;
        this.music_report_request_count = music_report_request_count;
        this.music_to_report = music_to_report;
        this.music_report_completed = music_report_completed;
        this.music_report_completed_time = music_report_completed_time;
        this.music_report_completed_action = music_report_completed_action;
        this.media_agency_planning = media_agency_planning;
        this.media_agency_buying = media_agency_buying;
        this.creative_agency = creative_agency;
        this.production_agency = production_agency;
        this.post_production_agency = post_production_agency;
        this.other_agency = other_agency;
        this.disabled_for_upload = disabled_for_upload;
        this.media_house_nr = media_house_nr;
        this.b_p_code = b_p_code;
        this.created_from_mismatch = created_from_mismatch;
        this.delivered_from_elsewhere = delivered_from_elsewhere;
        this.media_first_air_date = media_first_air_date;
        this.delivery_deadline = delivery_deadline;
        this.delivery_deadline_ms_algo = delivery_deadline_ms_algo;
        this.inactive = inactive;
        this.copy_envelope_id = copy_envelope_id;
        this.copy_envelope_action_state = copy_envelope_action_state;
        this.expiration_time = expiration_time;
        this.copy_envelope_sender_id = copy_envelope_sender_id;
        this.approval_advert_user_id = approval_advert_user_id;
        this.approval_advert_time = approval_advert_time;
        this.approval_advert_state = approval_advert_state;
        this.quality_check_state = quality_check_state;
        this.proxy_creation_state = proxy_creation_state;
        this.qc_proxy_approval_time = qc_proxy_approval_time;
        this.transcoding = transcoding;
        this.delivery_id = delivery_id;
        this.delivery_action_state = delivery_action_state;
        this.time_delivered = time_delivered;
        this.slave_time_delivered = slave_time_delivered;
        this.pre_delivered_time = pre_delivered_time;
        this.slave_pre_delivered_time = slave_pre_delivered_time;
        this.post_delivered_time = post_delivered_time;
        this.slave_post_delivered_time = slave_post_delivered_time;
        this.content_approval_media_user_id = content_approval_media_user_id;
        this.content_approval_media_time = content_approval_media_time;
        this.content_approval_media_state = content_approval_media_state;
        this.qc_approval_media_user_id = qc_approval_media_user_id;
        this.qc_approval_media_time = qc_approval_media_time;
        this.qc_approval_media_state = qc_approval_media_state;
        this.clock_approval_media_user_id = clock_approval_media_user_id;
        this.clock_approval_media_time = clock_approval_media_time;
        this.clock_approval_media_state = clock_approval_media_state;
        this.class_approval_media_user_id = class_approval_media_user_id;
        this.class_approval_media_time = class_approval_media_time;
        this.class_approval_media_state = class_approval_media_state;
        this.num_instruction_approved = num_instruction_approved;
        this.num_instruction_rejected = num_instruction_rejected;
        this.num_instruction_approval_not_performed = num_instruction_approval_not_performed;
        this.download_speed = download_speed;
        this.mismatch = mismatch;
        this.mismatch_ignored = mismatch_ignored;
        this.invoice_account_demand = invoice_account_demand;
    }

    @Override
    public int getClassId() {
        return PORTABLE_CLASS_ID;
    }

    @Override
    public int getFactoryId() {
        return PORTABLE_FACTORY_ID;
    }

    @Override
    public void writePortable(PortableWriter out) throws IOException {
        out.writeInt("UUID", UUID);

        out.writeLong("locality_id", locality_id);
        out.writeUTF("locality_name", locality_name);
        out.writeLong("division_id", division_id);
        out.writeUTF("division_name", division_name);
        out.writeLong("brand_id", brand_id);
        out.writeUTF("brand_name", brand_name);
        out.writeLong("media_id", media_id);
        out.writeUTF("media_name", media_name);
        out.writeInt("media_content_approval_instruction", media_content_approval_instruction);
        out.writeInt("media_qc_approval_instruction", media_qc_approval_instruction);
        out.writeInt("media_clock_approval_instruction", media_clock_approval_instruction);
        out.writeInt("media_class_approval_instruction", media_class_approval_instruction);
        out.writeInt("media_instruction_approval_instruction", media_instruction_approval_instruction);
        out.writeInt("media_house_nr_instruction", media_house_nr_instruction);
        out.writeBoolean("media_music_report_required", media_music_report_required);
        out.writeInt("media_delivery_hours", media_delivery_hours);
        out.writeLong("media_destination_id", media_destination_id);
        out.writeLong("media_slave_destination_id", media_slave_destination_id);

        out.writeLong("copy_code_id", copy_code_id);
        out.writeLong("region_id", region_id);
        out.writeLong("media_type_media_extension_id", media_type_media_extension_id);
        out.writeUTF("copy_code", copy_code);
        out.writeBoolean("aired", aired);
        out.writeLong("poster_id", poster_id);
        out.writeInt("film_length", film_length);
        out.writeLong("first_air_date", first_air_date);
        out.writeInt("content_type", content_type);
        out.writeUTF("title", title);
        out.writeBoolean("stopped", stopped);
        out.writeBoolean("request_upload", request_upload);
        out.writeLong("urr_time", urr_time);

        out.writeLong("sponsorship_locality_id", sponsorship_locality_id);
        out.writeLong("sponsorship_division_id", sponsorship_division_id);
        out.writeLong("music_report_request_time", music_report_request_time);
        out.writeInt("music_report_request_count", music_report_request_count);
        out.writeInt("music_to_report", music_to_report);
        out.writeBoolean("music_report_completed", music_report_completed);
        out.writeLong("music_report_completed_time", music_report_completed_time);
        out.writeBoolean("music_report_completed_action", music_report_completed_action);
        out.writeUTF("media_agency_planning", media_agency_planning);
        out.writeUTF("media_agency_buying", media_agency_buying);
        out.writeUTF("creative_agency", creative_agency);
        out.writeUTF("production_agency", production_agency);
        out.writeUTF("post_production_agency", post_production_agency);
        out.writeUTF("other_agency", other_agency);
        out.writeBoolean("disabled_for_upload", disabled_for_upload);

        out.writeUTF("media_house_nr", media_house_nr);
        out.writeUTF("b_p_code", b_p_code);
        out.writeBoolean("created_from_mismatch", created_from_mismatch);
        out.writeBoolean("delivered_from_elsewhere", delivered_from_elsewhere);
        out.writeLong("media_first_air_date", media_first_air_date);
        out.writeLong("delivery_deadline", delivery_deadline);
        out.writeLong("delivery_deadline_ms_algo", delivery_deadline_ms_algo);
        out.writeBoolean("inactive", inactive);

        out.writeLong("copy_envelope_id", copy_envelope_id);
        out.writeInt("copy_envelope_action_state", copy_envelope_action_state);
        out.writeLong("expiration_time", expiration_time);
        out.writeLong("copy_envelope_sender_id", copy_envelope_sender_id);
        out.writeLong("approval_advert_user_id", approval_advert_user_id);
        out.writeLong("approval_advert_time", approval_advert_time);
        out.writeInt("approval_advert_state", approval_advert_state);
        out.writeInt("quality_check_state", quality_check_state);
        out.writeInt("proxy_creation_state", proxy_creation_state);
        out.writeLong("qc_proxy_approval_time", qc_proxy_approval_time);
        out.writeBoolean("transcoding", transcoding);

        out.writeLong("delivery_id", delivery_id);
        out.writeInt("delivery_action_state", delivery_action_state);
        out.writeLong("time_delivered", time_delivered);
        out.writeLong("slave_time_delivered", slave_time_delivered);
        out.writeLong("pre_delivered_time", pre_delivered_time);
        out.writeLong("slave_pre_delivered_time", slave_pre_delivered_time);
        out.writeLong("post_delivered_time", post_delivered_time);
        out.writeLong("slave_post_delivered_time", slave_post_delivered_time);

        out.writeLong("content_approval_media_user_id", content_approval_media_user_id);
        out.writeLong("content_approval_media_time", content_approval_media_time);
        out.writeInt("content_approval_media_state", content_approval_media_state);

        out.writeLong("qc_approval_media_user_id", qc_approval_media_user_id);
        out.writeLong("qc_approval_media_time", qc_approval_media_time);
        out.writeInt("qc_approval_media_state", qc_approval_media_state);

        out.writeLong("clock_approval_media_user_id", clock_approval_media_user_id);
        out.writeLong("clock_approval_media_time", clock_approval_media_time);
        out.writeInt("clock_approval_media_state", clock_approval_media_state);

        out.writeLong("class_approval_media_user_id", class_approval_media_user_id);
        out.writeLong("class_approval_media_time", class_approval_media_time);
        out.writeInt("class_approval_media_state", class_approval_media_state);
        out.writeInt("num_instruction_approved", num_instruction_approved);
        out.writeInt("num_instruction_rejected", num_instruction_rejected);
        out.writeInt("num_instruction_approval_not_performed", num_instruction_approval_not_performed);
        out.writeLong("download_speed", download_speed);
        out.writeInt("mismatch", mismatch);
        out.writeBoolean("mismatch_ignored", mismatch_ignored);
        out.writeInt("invoice_account_demand", invoice_account_demand);
    }

    public void readPortable(PortableReader in) throws IOException {
        UUID = in.readInt("UUID");
        locality_id = in.readLong("locality_id");
        locality_name = in.readUTF("locality_name");
        division_id = in.readLong("division_id");
        division_name = in.readUTF("division_name");
        brand_id = in.readLong("brand_id");
        brand_name = in.readUTF("brand_name");
        media_id = in.readLong("media_id");
        media_name = in.readUTF("media_name");
        media_content_approval_instruction = in.readInt("media_content_approval_instruction");
        media_qc_approval_instruction = in.readInt("media_qc_approval_instruction");
        media_clock_approval_instruction = in.readInt("media_clock_approval_instruction");
        media_class_approval_instruction = in.readInt("media_class_approval_instruction");
        media_instruction_approval_instruction = in.readInt("media_instruction_approval_instruction");
        media_house_nr_instruction = in.readInt("media_house_nr_instruction");
        media_music_report_required = in.readBoolean("media_music_report_required");
        media_delivery_hours = in.readInt("media_delivery_hours");
        media_destination_id = in.readLong("media_destination_id");
        media_slave_destination_id = in.readLong("media_slave_destination_id");

        copy_code_id = in.readLong("copy_code_id");
        region_id = in.readLong("region_id");
        media_type_media_extension_id = in.readLong("media_type_media_extension_id");
        copy_code = in.readUTF("copy_code");
        aired = in.readBoolean("aired");
        poster_id = in.readLong("poster_id");
        film_length = in.readInt("film_length");
        first_air_date = in.readLong("first_air_date");
        content_type = in.readInt("content_type");
        title = in.readUTF("title");
        stopped = in.readBoolean("stopped");
        request_upload = in.readBoolean("request_upload");
        urr_time = in.readLong("urr_time");

        sponsorship_locality_id = in.readLong("sponsorship_locality_id");
        sponsorship_division_id = in.readLong("sponsorship_division_id");
        music_report_request_time = in.readLong("music_report_request_time");
        music_report_request_count = in.readInt("music_report_request_count");
        music_to_report = in.readInt("music_to_report");
        music_report_completed = in.readBoolean("music_report_completed");
        music_report_completed_time = in.readLong("music_report_completed_time");
        music_report_completed_action = in.readBoolean("music_report_completed_action");
        media_agency_planning = in.readUTF("media_agency_planning");
        media_agency_buying = in.readUTF("media_agency_buying");
        creative_agency = in.readUTF("creative_agency");
        production_agency = in.readUTF("production_agency");
        post_production_agency = in.readUTF("post_production_agency");
        other_agency = in.readUTF("other_agency");
        disabled_for_upload = in.readBoolean("disabled_for_upload");

        media_house_nr = in.readUTF("media_house_nr");
        b_p_code = in.readUTF("b_p_code");
        created_from_mismatch = in.readBoolean("created_from_mismatch");
        delivered_from_elsewhere = in.readBoolean("delivered_from_elsewhere");
        media_first_air_date = in.readLong("media_first_air_date");
        delivery_deadline = in.readLong("delivery_deadline");
        delivery_deadline_ms_algo = in.readLong("delivery_deadline_ms_algo");
        inactive = in.readBoolean("inactive");

        copy_envelope_id = in.readLong("copy_envelope_id");
        copy_envelope_action_state = in.readInt("copy_envelope_action_state");
        expiration_time = in.readLong("expiration_time");
        copy_envelope_sender_id = in.readLong("copy_envelope_sender_id");
        approval_advert_user_id = in.readLong("approval_advert_user_id");
        approval_advert_time = in.readLong("approval_advert_time");
        approval_advert_state = in.readInt("approval_advert_state");
        quality_check_state = in.readInt("quality_check_state");
        proxy_creation_state = in.readInt("proxy_creation_state");
        qc_proxy_approval_time = in.readLong("qc_proxy_approval_time");
        transcoding = in.readBoolean("transcoding");

        delivery_id = in.readLong("delivery_id");
        delivery_action_state = in.readInt("delivery_action_state");
        time_delivered = in.readLong("time_delivered");
        slave_time_delivered = in.readLong("slave_time_delivered");
        pre_delivered_time = in.readLong("pre_delivered_time");
        slave_pre_delivered_time = in.readLong("slave_pre_delivered_time");
        post_delivered_time = in.readLong("post_delivered_time");
        slave_post_delivered_time = in.readLong("slave_post_delivered_time");

        content_approval_media_user_id = in.readLong("content_approval_media_user_id");
        content_approval_media_time = in.readLong("content_approval_media_time");
        content_approval_media_state = in.readInt("content_approval_media_state");

        qc_approval_media_user_id = in.readLong("qc_approval_media_user_id");
        qc_approval_media_time = in.readLong("qc_approval_media_time");
        qc_approval_media_state = in.readInt("qc_approval_media_state");

        clock_approval_media_user_id = in.readLong("clock_approval_media_user_id");
        clock_approval_media_time = in.readLong("clock_approval_media_time");
        clock_approval_media_state = in.readInt("clock_approval_media_state");

        class_approval_media_user_id = in.readLong("class_approval_media_user_id");
        class_approval_media_time = in.readLong("class_approval_media_time");
        class_approval_media_state = in.readInt("class_approval_media_state");
        num_instruction_approved = in.readInt("num_instruction_approved");
        num_instruction_rejected = in.readInt("num_instruction_rejected");
        num_instruction_approval_not_performed = in.readInt("num_instruction_approval_not_performed");
        download_speed = in.readLong("download_speed");
        mismatch = in.readInt("mismatch");
        mismatch_ignored = in.readBoolean("mismatch_ignored");
        invoice_account_demand = in.readInt("invoice_account_demand");
    }

    public void setLocality_id(long locality_id) {
        this.locality_id = locality_id;
    }

    public void setLocality_name(String locality_name) {
        this.locality_name = locality_name;
    }

    public void setDivision_id(long division_id) {
        this.division_id = division_id;
    }

    public void setDivision_name(String division_name) {
        this.division_name = division_name;
    }

    public void setBrand_id(long brand_id) {
        this.brand_id = brand_id;
    }

    public void setBrand_name(String brand_name) {
        this.brand_name = brand_name;
    }

    public void setMedia_id(long media_id) {
        this.media_id = media_id;
    }

    public void setMedia_name(String media_name) {
        this.media_name = media_name;
    }

    public void setMedia_content_approval_instruction(
            int media_content_approval_instruction) {
        this.media_content_approval_instruction = media_content_approval_instruction;
    }

    public void setMedia_qc_approval_instruction(int media_qc_approval_instruction) {
        this.media_qc_approval_instruction = media_qc_approval_instruction;
    }

    public void setMedia_clock_approval_instruction(
            int media_clock_approval_instruction) {
        this.media_clock_approval_instruction = media_clock_approval_instruction;
    }

    public void setMedia_class_approval_instruction(
            int media_class_approval_instruction) {
        this.media_class_approval_instruction = media_class_approval_instruction;
    }

    public void setMedia_instruction_approval_instruction(
            int media_instruction_approval_instruction) {
        this.media_instruction_approval_instruction = media_instruction_approval_instruction;
    }

    public void setMedia_house_nr_instruction(int media_house_nr_instruction) {
        this.media_house_nr_instruction = media_house_nr_instruction;
    }

    public void setMedia_music_report_required(boolean media_music_report_required) {
        this.media_music_report_required = media_music_report_required;
    }

    public void setMedia_delivery_hours(int media_delivery_hours) {
        this.media_delivery_hours = media_delivery_hours;
    }

    public void setMedia_destination_id(long media_destination_id) {
        this.media_destination_id = media_destination_id;
    }

    public void setMedia_slave_destination_id(long media_slave_destination_id) {
        this.media_slave_destination_id = media_slave_destination_id;
    }

    public void setCopy_code_id(long copy_code_id) {
        this.copy_code_id = copy_code_id;
    }

    public void setRegion_id(long region_id) {
        this.region_id = region_id;
    }

    public void setMedia_type_media_extension_id(long media_type_media_extension_id) {
        this.media_type_media_extension_id = media_type_media_extension_id;
    }

    public void setCopy_code(String copy_code) {
        this.copy_code = copy_code;
    }

    public void setAired(boolean aired) {
        this.aired = aired;
    }

    public void setPoster_id(long poster_id) {
        this.poster_id = poster_id;
    }

    public void setFilm_length(int film_length) {
        this.film_length = film_length;
    }

    public void setFirst_air_date(long first_air_date) {
        this.first_air_date = first_air_date;
    }

    public void setContent_type(int content_type) {
        this.content_type = content_type;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    public void setRequest_upload(boolean request_upload) {
        this.request_upload = request_upload;
    }

    public void setUrr_time(long urr_time) {
        this.urr_time = urr_time;
    }


    public void setSponsorship_locality_id(long sponsorship_locality_id) {
        this.sponsorship_locality_id = sponsorship_locality_id;
    }

    public void setSponsorship_division_id(long sponsorship_division_id) {
        this.sponsorship_division_id = sponsorship_division_id;
    }

    public void setMusic_report_request_time(long music_report_request_time) {
        this.music_report_request_time = music_report_request_time;
    }

    public void setMusic_report_request_count(int music_report_request_count) {
        this.music_report_request_count = music_report_request_count;
    }

    public void setMusic_to_report(int music_to_report) {
        this.music_to_report = music_to_report;
    }

    public void setMusic_report_completed(boolean music_report_completed) {
        this.music_report_completed = music_report_completed;
    }

    public void setMusic_report_completed_time(long music_report_completed_time) {
        this.music_report_completed_time = music_report_completed_time;
    }

    public void setMusic_report_completed_action(
            boolean music_report_completed_action) {
        this.music_report_completed_action = music_report_completed_action;
    }

    public void setMedia_agency_planning(String media_agency_planning) {
        this.media_agency_planning = media_agency_planning;
    }

    public void setMedia_agency_buying(String media_agency_buying) {
        this.media_agency_buying = media_agency_buying;
    }

    public void setCreative_agency(String creative_agency) {
        this.creative_agency = creative_agency;
    }

    public void setProduction_agency(String production_agency) {
        this.production_agency = production_agency;
    }

    public void setPost_production_agency(String post_production_agency) {
        this.post_production_agency = post_production_agency;
    }

    public void setOther_agency(String other_agency) {
        this.other_agency = other_agency;
    }

    public void setDisabled_for_upload(boolean disabled_for_upload) {
        this.disabled_for_upload = disabled_for_upload;
    }

    public void setMedia_house_nr(String media_house_nr) {
        this.media_house_nr = media_house_nr;
    }

    public void setB_p_code(String b_p_code) {
        this.b_p_code = b_p_code;
    }

    public void setCreated_from_mismatch(boolean created_from_mismatch) {
        this.created_from_mismatch = created_from_mismatch;
    }

    public void setDelivered_from_elsewhere(boolean delivered_from_elsewhere) {
        this.delivered_from_elsewhere = delivered_from_elsewhere;
    }

    public void setMedia_first_air_date(long media_first_air_date) {
        this.media_first_air_date = media_first_air_date;
    }

    public void setDelivery_deadline(long delivery_deadline) {
        this.delivery_deadline = delivery_deadline;
    }

    public void setDelivery_deadline_ms_algo(long delivery_deadline_ms_algo) {
        this.delivery_deadline_ms_algo = delivery_deadline_ms_algo;
    }

    public void setInactive(boolean inactive) {
        this.inactive = inactive;
    }

    public void setCopy_envelope_id(long copy_envelope_id) {
        this.copy_envelope_id = copy_envelope_id;
    }

    public void setCopy_envelope_action_state(int copy_envelope_action_state) {
        this.copy_envelope_action_state = copy_envelope_action_state;
    }

    public void setExpiration_time(long expiration_time) {
        this.expiration_time = expiration_time;
    }

    public void setCopy_envelope_sender_id(long copy_envelope_sender_id) {
        this.copy_envelope_sender_id = copy_envelope_sender_id;
    }

    public void setApproval_advert_user_id(long approval_advert_user_id) {
        this.approval_advert_user_id = approval_advert_user_id;
    }

    public void setApproval_advert_time(long approval_advert_time) {
        this.approval_advert_time = approval_advert_time;
    }

    public void setApproval_advert_state(int approval_advert_state) {
        this.approval_advert_state = approval_advert_state;
    }

    public void setQuality_check_state(int quality_check_state) {
        this.quality_check_state = quality_check_state;
    }

    public void setProxy_creation_state(int proxy_creation_state) {
        this.proxy_creation_state = proxy_creation_state;
    }

    public void setQc_proxy_approval_time(long qc_proxy_approval_time) {
        this.qc_proxy_approval_time = qc_proxy_approval_time;
    }

    public void setTranscoding(boolean transcoding) {
        this.transcoding = transcoding;
    }

    public void setDelivery_id(long delivery_id) {
        this.delivery_id = delivery_id;
    }

    public void setDelivery_action_state(int delivery_action_state) {
        this.delivery_action_state = delivery_action_state;
    }

    public void setTime_delivered(long time_delivered) {
        this.time_delivered = time_delivered;
    }

    public void setSlave_time_delivered(long slave_time_delivered) {
        this.slave_time_delivered = slave_time_delivered;
    }

    public void setPre_delivered_time(long pre_delivered_time) {
        this.pre_delivered_time = pre_delivered_time;
    }

    public void setSlave_pre_delivered_time(long slave_pre_delivered_time) {
        this.slave_pre_delivered_time = slave_pre_delivered_time;
    }

    public void setPost_delivered_time(long post_delivered_time) {
        this.post_delivered_time = post_delivered_time;
    }

    public void setSlave_post_delivered_time(long slave_post_delivered_time) {
        this.slave_post_delivered_time = slave_post_delivered_time;
    }

    public void setContent_approval_media_user_id(
            long content_approval_media_user_id) {
        this.content_approval_media_user_id = content_approval_media_user_id;
    }

    public void setContent_approval_media_time(long content_approval_media_time) {
        this.content_approval_media_time = content_approval_media_time;
    }

    public void setContent_approval_media_state(int content_approval_media_state) {
        this.content_approval_media_state = content_approval_media_state;
    }

    public void setQc_approval_media_user_id(long qc_approval_media_user_id) {
        this.qc_approval_media_user_id = qc_approval_media_user_id;
    }

    public void setQc_approval_media_time(long qc_approval_media_time) {
        this.qc_approval_media_time = qc_approval_media_time;
    }

    public void setQc_approval_media_state(int qc_approval_media_state) {
        this.qc_approval_media_state = qc_approval_media_state;
    }

    public void setClock_approval_media_user_id(long clock_approval_media_user_id) {
        this.clock_approval_media_user_id = clock_approval_media_user_id;
    }

    public void setClock_approval_media_time(long clock_approval_media_time) {
        this.clock_approval_media_time = clock_approval_media_time;
    }

    public void setClock_approval_media_state(int clock_approval_media_state) {
        this.clock_approval_media_state = clock_approval_media_state;
    }

    public void setClass_approval_media_user_id(long class_approval_media_user_id) {
        this.class_approval_media_user_id = class_approval_media_user_id;
    }

    public void setClass_approval_media_time(long class_approval_media_time) {
        this.class_approval_media_time = class_approval_media_time;
    }

    public void setClass_approval_media_state(int class_approval_media_state) {
        this.class_approval_media_state = class_approval_media_state;
    }

    public void setNum_instruction_approved(int num_instruction_approved) {
        this.num_instruction_approved = num_instruction_approved;
    }

    public void setNum_instruction_rejected(int num_instruction_rejected) {
        this.num_instruction_rejected = num_instruction_rejected;
    }

    public void setNum_instruction_approval_not_performed(int num_instruction_approval_not_performed) {
        this.num_instruction_approval_not_performed = num_instruction_approval_not_performed;
    }

    public void setDownload_speed(long download_speed) {
        this.download_speed = download_speed;
    }

    public void setMismatch(int mismatch) {
        this.mismatch = mismatch;
    }

    public void setMismatch_ignored(boolean mismatch_ignored) {
        this.mismatch_ignored = mismatch_ignored;
    }

    public void setInvoice_account_demand(int invoice_account_demand) {
        this.invoice_account_demand = invoice_account_demand;
    }

    public String getQuickSearchKey() {
        return format("%d_%d_%d", copy_code_id, media_id, brand_id);
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (aired ? 1231 : 1237);
        result = prime * result + approval_advert_state;
        result = prime * result + (int) (approval_advert_time ^ (approval_advert_time >>> 32));
        result = prime * result + (int) (approval_advert_user_id ^ (approval_advert_user_id >>> 32));
        result = prime * result + ((b_p_code == null) ? 0 : b_p_code.hashCode());
        result = prime * result + (int) (brand_id ^ (brand_id >>> 32));
        result = prime * result + ((brand_name == null) ? 0 : brand_name.hashCode());
        result = prime * result + class_approval_media_state;
        result = prime * result + (int) (class_approval_media_time ^ (class_approval_media_time >>> 32));
        result = prime * result + (int) (class_approval_media_user_id ^ (class_approval_media_user_id >>> 32));
        result = prime * result + clock_approval_media_state;
        result = prime * result + (int) (clock_approval_media_time ^ (clock_approval_media_time >>> 32));
        result = prime * result + (int) (clock_approval_media_user_id ^ (clock_approval_media_user_id >>> 32));
        result = prime * result + content_approval_media_state;
        result = prime * result + (int) (content_approval_media_time ^ (content_approval_media_time >>> 32));
        result = prime * result + (int) (content_approval_media_user_id ^ (content_approval_media_user_id >>> 32));
        result = prime * result + content_type;
        result = prime * result + ((copy_code == null) ? 0 : copy_code.hashCode());
        result = prime * result + (int) (copy_code_id ^ (copy_code_id >>> 32));
        result = prime * result + copy_envelope_action_state;
        result = prime * result + (int) (copy_envelope_id ^ (copy_envelope_id >>> 32));
        result = prime * result + (int) (copy_envelope_sender_id ^ (copy_envelope_sender_id >>> 32));
        result = prime * result + (created_from_mismatch ? 1231 : 1237);
        result = prime * result + ((creative_agency == null) ? 0 : creative_agency.hashCode());
        result = prime * result + (delivered_from_elsewhere ? 1231 : 1237);
        result = prime * result + delivery_action_state;
        result = prime * result + (int) (delivery_deadline ^ (delivery_deadline >>> 32));
        result = prime * result + (int) (delivery_deadline_ms_algo ^ (delivery_deadline_ms_algo >>> 32));
        result = prime * result + (int) (delivery_id ^ (delivery_id >>> 32));
        result = prime * result + (disabled_for_upload ? 1231 : 1237);
        result = prime * result + (int) (division_id ^ (division_id >>> 32));
        result = prime * result + ((division_name == null) ? 0 : division_name.hashCode());
        result = prime * result + (int) (download_speed ^ (download_speed >>> 32));
        result = prime * result + (int) (expiration_time ^ (expiration_time >>> 32));
        result = prime * result + film_length;
        result = prime * result + (int) (first_air_date ^ (first_air_date >>> 32));
        result = prime * result + (inactive ? 1231 : 1237);
        result = prime * result + invoice_account_demand;
        result = prime * result + (int) (locality_id ^ (locality_id >>> 32));
        result = prime * result + ((locality_name == null) ? 0 : locality_name.hashCode());
        result = prime * result + ((media_agency_buying == null) ? 0 : media_agency_buying.hashCode());
        result = prime * result + ((media_agency_planning == null) ? 0 : media_agency_planning.hashCode());
        result = prime * result + media_class_approval_instruction;
        result = prime * result + media_clock_approval_instruction;
        result = prime * result + media_content_approval_instruction;
        result = prime * result + media_delivery_hours;
        result = prime * result + (int) (media_destination_id ^ (media_destination_id >>> 32));
        result = prime * result + (int) (media_first_air_date ^ (media_first_air_date >>> 32));
        result = prime * result + ((media_house_nr == null) ? 0 : media_house_nr.hashCode());
        result = prime * result + media_house_nr_instruction;
        result = prime * result + (int) (media_id ^ (media_id >>> 32));
        result = prime * result + media_instruction_approval_instruction;
        result = prime * result + (media_music_report_required ? 1231 : 1237);
        result = prime * result + ((media_name == null) ? 0 : media_name.hashCode());
        result = prime * result + media_qc_approval_instruction;
        result = prime * result + (int) (media_slave_destination_id ^ (media_slave_destination_id >>> 32));
        result = prime * result + (int) (media_type_media_extension_id ^ (media_type_media_extension_id >>> 32));
        result = prime * result + mismatch;
        result = prime * result + (mismatch_ignored ? 1231 : 1237);
        result = prime * result + (music_report_completed ? 1231 : 1237);
        result = prime * result + (music_report_completed_action ? 1231 : 1237);
        result = prime * result + (int) (music_report_completed_time ^ (music_report_completed_time >>> 32));
        result = prime * result + music_report_request_count;
        result = prime * result + (int) (music_report_request_time ^ (music_report_request_time >>> 32));
        result = prime * result + music_to_report;
        result = prime * result + num_instruction_approval_not_performed;
        result = prime * result + num_instruction_approved;
        result = prime * result + num_instruction_rejected;
        result = prime * result + ((other_agency == null) ? 0 : other_agency.hashCode());
        result = prime * result + (int) (post_delivered_time ^ (post_delivered_time >>> 32));
        result = prime * result + ((post_production_agency == null) ? 0 : post_production_agency.hashCode());
        result = prime * result + (int) (poster_id ^ (poster_id >>> 32));
        result = prime * result + (int) (pre_delivered_time ^ (pre_delivered_time >>> 32));
        result = prime * result + ((production_agency == null) ? 0 : production_agency.hashCode());
        result = prime * result + proxy_creation_state;
        result = prime * result + qc_approval_media_state;
        result = prime * result + (int) (qc_approval_media_time ^ (qc_approval_media_time >>> 32));
        result = prime * result + (int) (qc_approval_media_user_id ^ (qc_approval_media_user_id >>> 32));
        result = prime * result + (int) (qc_proxy_approval_time ^ (qc_proxy_approval_time >>> 32));
        result = prime * result + quality_check_state;
        result = prime * result + (int) (region_id ^ (region_id >>> 32));
        result = prime * result + (request_upload ? 1231 : 1237);
        result = prime * result + (int) (slave_post_delivered_time ^ (slave_post_delivered_time >>> 32));
        result = prime * result + (int) (slave_pre_delivered_time ^ (slave_pre_delivered_time >>> 32));
        result = prime * result + (int) (slave_time_delivered ^ (slave_time_delivered >>> 32));
        result = prime * result + (int) (sponsorship_division_id ^ (sponsorship_division_id >>> 32));
        result = prime * result + (int) (sponsorship_locality_id ^ (sponsorship_locality_id >>> 32));
        result = prime * result + (stopped ? 1231 : 1237);
        result = prime * result + (int) (time_delivered ^ (time_delivered >>> 32));
        result = prime * result + ((title == null) ? 0 : title.hashCode());
        result = prime * result + (transcoding ? 1231 : 1237);
        result = prime * result + (int) (urr_time ^ (urr_time >>> 32));
        return result;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ComplexDomainObject other = (ComplexDomainObject) obj;

        if (UUID != other.UUID) {
            return false;
        }

        if (aired != other.aired) {
            return false;
        }
        if (approval_advert_state != other.approval_advert_state) {
            return false;
        }
        if (approval_advert_time != other.approval_advert_time) {
            return false;
        }
        if (approval_advert_user_id != other.approval_advert_user_id) {
            return false;
        }
        if (b_p_code == null) {
            if (other.b_p_code != null) {
                return false;
            }
        } else if (!b_p_code.equals(other.b_p_code)) {
            return false;
        }
        if (brand_id != other.brand_id) {
            return false;
        }
        if (brand_name == null) {
            if (other.brand_name != null) {
                return false;
            }
        } else if (!brand_name.equals(other.brand_name)) {
            return false;
        }
        if (class_approval_media_state != other.class_approval_media_state) {
            return false;
        }
        if (class_approval_media_time != other.class_approval_media_time) {
            return false;
        }
        if (class_approval_media_user_id != other.class_approval_media_user_id) {
            return false;
        }
        if (clock_approval_media_state != other.clock_approval_media_state) {
            return false;
        }
        if (clock_approval_media_time != other.clock_approval_media_time) {
            return false;
        }
        if (clock_approval_media_user_id != other.clock_approval_media_user_id) {
            return false;
        }
        if (content_approval_media_state != other.content_approval_media_state) {
            return false;
        }
        if (content_approval_media_time != other.content_approval_media_time) {
            return false;
        }
        if (content_approval_media_user_id != other.content_approval_media_user_id) {
            return false;
        }
        if (content_type != other.content_type) {
            return false;
        }
        if (copy_code == null) {
            if (other.copy_code != null) {
                return false;
            }
        } else if (!copy_code.equals(other.copy_code)) {
            return false;
        }
        if (copy_code_id != other.copy_code_id) {
            return false;
        }
        if (copy_envelope_action_state != other.copy_envelope_action_state) {
            return false;
        }
        if (copy_envelope_id != other.copy_envelope_id) {
            return false;
        }
        if (copy_envelope_sender_id != other.copy_envelope_sender_id) {
            return false;
        }
        if (created_from_mismatch != other.created_from_mismatch) {
            return false;
        }
        if (creative_agency == null) {
            if (other.creative_agency != null) {
                return false;
            }
        } else if (!creative_agency.equals(other.creative_agency)) {
            return false;
        }
        if (delivered_from_elsewhere != other.delivered_from_elsewhere) {
            return false;
        }
        if (delivery_action_state != other.delivery_action_state) {
            return false;
        }
        if (delivery_deadline != other.delivery_deadline) {
            return false;
        }
        if (delivery_deadline_ms_algo != other.delivery_deadline_ms_algo) {
            return false;
        }
        if (delivery_id != other.delivery_id) {
            return false;
        }
        if (disabled_for_upload != other.disabled_for_upload) {
            return false;
        }
        if (division_id != other.division_id) {
            return false;
        }
        if (division_name == null) {
            if (other.division_name != null) {
                return false;
            }
        } else if (!division_name.equals(other.division_name)) {
            return false;
        }
        if (download_speed != other.download_speed) {
            return false;
        }
        if (expiration_time != other.expiration_time) {
            return false;
        }
        if (film_length != other.film_length) {
            return false;
        }
        if (first_air_date != other.first_air_date) {
            return false;
        }
        if (inactive != other.inactive) {
            return false;
        }
        if (invoice_account_demand != other.invoice_account_demand) {
            return false;
        }
        if (locality_id != other.locality_id) {
            return false;
        }
        if (locality_name == null) {
            if (other.locality_name != null) {
                return false;
            }
        } else if (!locality_name.equals(other.locality_name)) {
            return false;
        }
        if (media_agency_buying == null) {
            if (other.media_agency_buying != null) {
                return false;
            }
        } else if (!media_agency_buying.equals(other.media_agency_buying)) {
            return false;
        }
        if (media_agency_planning == null) {
            if (other.media_agency_planning != null) {
                return false;
            }
        } else if (!media_agency_planning
                .equals(other.media_agency_planning)) {
            return false;
        }
        if (media_class_approval_instruction != other.media_class_approval_instruction) {
            return false;
        }
        if (media_clock_approval_instruction != other.media_clock_approval_instruction) {
            return false;
        }
        if (media_content_approval_instruction != other.media_content_approval_instruction) {
            return false;
        }
        if (media_delivery_hours != other.media_delivery_hours) {
            return false;
        }
        if (media_destination_id != other.media_destination_id) {
            return false;
        }
        if (media_first_air_date != other.media_first_air_date) {
            return false;
        }
        if (media_house_nr == null) {
            if (other.media_house_nr != null) {
                return false;
            }
        } else if (!media_house_nr.equals(other.media_house_nr)) {
            return false;
        }
        if (media_house_nr_instruction != other.media_house_nr_instruction) {
            return false;
        }
        if (media_id != other.media_id) {
            return false;
        }
        if (media_instruction_approval_instruction != other.media_instruction_approval_instruction) {
            return false;
        }
        if (media_music_report_required != other.media_music_report_required) {
            return false;
        }
        if (media_name == null) {
            if (other.media_name != null) {
                return false;
            }
        } else if (!media_name.equals(other.media_name)) {
            return false;
        }
        if (media_qc_approval_instruction != other.media_qc_approval_instruction) {
            return false;
        }
        if (media_slave_destination_id != other.media_slave_destination_id) {
            return false;
        }
        if (media_type_media_extension_id != other.media_type_media_extension_id) {
            return false;
        }
        if (mismatch != other.mismatch) {
            return false;
        }
        if (mismatch_ignored != other.mismatch_ignored) {
            return false;
        }
        if (music_report_completed != other.music_report_completed) {
            return false;
        }
        if (music_report_completed_action != other.music_report_completed_action) {
            return false;
        }
        if (music_report_completed_time != other.music_report_completed_time) {
            return false;
        }
        if (music_report_request_count != other.music_report_request_count) {
            return false;
        }
        if (music_report_request_time != other.music_report_request_time) {
            return false;
        }
        if (music_to_report != other.music_to_report) {
            return false;
        }
        if (num_instruction_approval_not_performed != other.num_instruction_approval_not_performed) {
            return false;
        }
        if (num_instruction_approved != other.num_instruction_approved) {
            return false;
        }
        if (num_instruction_rejected != other.num_instruction_rejected) {
            return false;
        }
        if (other_agency == null) {
            if (other.other_agency != null) {
                return false;
            }
        } else if (!other_agency.equals(other.other_agency)) {
            return false;
        }
        if (post_delivered_time != other.post_delivered_time) {
            return false;
        }
        if (post_production_agency == null) {
            if (other.post_production_agency != null) {
                return false;
            }
        } else if (!post_production_agency
                .equals(other.post_production_agency)) {
            return false;
        }
        if (poster_id != other.poster_id) {
            return false;
        }
        if (pre_delivered_time != other.pre_delivered_time) {
            return false;
        }
        if (production_agency == null) {
            if (other.production_agency != null) {
                return false;
            }
        } else if (!production_agency.equals(other.production_agency)) {
            return false;
        }
        if (proxy_creation_state != other.proxy_creation_state) {
            return false;
        }
        if (qc_approval_media_state != other.qc_approval_media_state) {
            return false;
        }
        if (qc_approval_media_time != other.qc_approval_media_time) {
            return false;
        }
        if (qc_approval_media_user_id != other.qc_approval_media_user_id) {
            return false;
        }
        if (qc_proxy_approval_time != other.qc_proxy_approval_time) {
            return false;
        }
        if (quality_check_state != other.quality_check_state) {
            return false;
        }
        if (region_id != other.region_id) {
            return false;
        }
        if (request_upload != other.request_upload) {
            return false;
        }
        if (slave_post_delivered_time != other.slave_post_delivered_time) {
            return false;
        }
        if (slave_pre_delivered_time != other.slave_pre_delivered_time) {
            return false;
        }
        if (slave_time_delivered != other.slave_time_delivered) {
            return false;
        }
        if (sponsorship_division_id != other.sponsorship_division_id) {
            return false;
        }
        if (sponsorship_locality_id != other.sponsorship_locality_id) {
            return false;
        }
        if (stopped != other.stopped) {
            return false;
        }
        if (time_delivered != other.time_delivered) {
            return false;
        }
        if (title == null) {
            if (other.title != null) {
                return false;
            }
        } else if (!title.equals(other.title)) {
            return false;
        }
        if (transcoding != other.transcoding) {
            return false;
        }
        return urr_time == other.urr_time;
    }

    @Override
    public String toString() {
        return "ComplexDomainObject{"
                + "title=" + title
                + ", copy_code=" + copy_code
                + ", copy_code_id=" + copy_code_id
                + ", film_length=" + film_length
                + '}';
    }
}
