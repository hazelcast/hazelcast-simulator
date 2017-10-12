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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.helpers.ComplexDomainObject;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Map;
import java.util.Random;
import java.util.Set;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.logging.Level.INFO;

@SuppressWarnings({"checkstyle:linelength", "checkstyle:trailingcomment"})
public class MapComplexPredicateTest extends AbstractTest {

    public enum QUERY_TYPE {

        QUERY1(QUERY_TYPE.QUERY1_SQL),
        QUERY2(QUERY_TYPE.QUERY2_SQL),
        QUERY3(QUERY_TYPE.QUERY3_SQL);

        private static final String QUERY1_SQL = "disabled_for_upload = false " + // done
                " AND (media_id != -1 AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 " + // done
                " AND ((media_content_approval_instruction != 3 AND content_approval_media_state != 19 AND content_approval_media_state != 17) " + // done
                " OR (media_clock_approval_instruction != 31 AND clock_approval_media_state != 41) OR (media_instruction_approval_instruction != 51 " + // done
                " AND (num_instruction_approval_not_performed > 0 OR num_instruction_rejected > 0)) OR (media_class_approval_instruction != 41 " + // done
                " AND class_approval_media_state != 71) OR (media_house_nr = '' AND media_house_nr_instruction = 10)) AND (content_approval_media_state != 18 " + // done
                " AND clock_approval_media_state != 42 AND class_approval_media_state != 72 AND num_instruction_rejected = 0 AND qc_approval_media_state != 22 " + // done
                " AND approval_advert_state != 11) AND (aired = false) AND (approval_advert_state = 10 OR approval_advert_state = 9) AND (NOT (time_delivered <= 0 " + // done
                " AND stopped = false AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 AND (content_approval_media_state = 19 " + // done
                " OR content_approval_media_state = 17 OR media_content_approval_instruction != 1) AND (class_approval_media_state = 71 OR media_class_approval_instruction != 40) " + // done
                "AND (clock_approval_media_state = 41 OR media_clock_approval_instruction != 30) " + // done
                "AND (media_instruction_approval_instruction != 50 OR (num_instruction_approval_not_performed = 0 AND num_instruction_rejected = 0)) " + // done
                "AND (approval_advert_state = 10 OR approval_advert_state = 9) AND (media_house_nr != '' OR media_house_nr_instruction = 11))) " + // done
                "AND (media_id > 60000000))";

        private static final String QUERY2_SQL = "disabled_for_upload = false "
                + " AND (media_id != -1 AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 "
                + " AND ((media_content_approval_instruction != 3 AND content_approval_media_state != 19 AND content_approval_media_state != 17) "
                + " OR (media_clock_approval_instruction != 31 AND clock_approval_media_state != 41) OR (media_instruction_approval_instruction != 51 "
                + " AND (num_instruction_approval_not_performed > 0 OR num_instruction_rejected > 0)) OR (media_class_approval_instruction != 41 "
                + " AND class_approval_media_state != 71) OR (media_house_nr = '' AND media_house_nr_instruction = 10)) AND (content_approval_media_state != 18 "
                + " AND clock_approval_media_state != 42 AND class_approval_media_state != 72 AND num_instruction_rejected = 0 AND qc_approval_media_state != 22 "
                + " AND approval_advert_state != 11) AND (aired = false) AND (approval_advert_state = 10 OR approval_advert_state = 9) AND (NOT (time_delivered <= 0 "
                + " AND stopped = false AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 AND (content_approval_media_state = 19 "
                + " OR content_approval_media_state = 17 OR media_content_approval_instruction != 1) AND (class_approval_media_state = 71 OR media_class_approval_instruction != 40) "
                + "AND (clock_approval_media_state = 41 OR media_clock_approval_instruction != 30) "
                + "AND (media_instruction_approval_instruction != 50 OR (num_instruction_approval_not_performed = 0 AND num_instruction_rejected = 0)) "
                + "AND (approval_advert_state = 10 OR approval_advert_state = 9) AND (media_house_nr != '' OR media_house_nr_instruction = 11))) "
                + "AND (media_id = 73474404 OR media_id = 59673386 OR media_id = 59673555 "
                + "OR media_id = 60386838 OR media_id = 60386991 OR media_id = 60387038 OR media_id = 63063042 OR media_id = 71930797 OR media_id = 71930974 OR media_id = 71931106 "
                + "OR media_id = 78158138 OR media_id = 121 OR media_id = 122 OR media_id = 123 OR media_id = 74724915 OR media_id = 59791484 OR media_id = 60474387 "
                + "OR media_id = 60995088 OR media_id = 62718106 OR media_id = 63355282 OR media_id = 63355295 OR media_id = 63803353 OR media_id = 64695309 OR media_id = 67657712 "
                + "OR media_id = 72587686 OR media_id = 62017377 ))"
                + "OR media_id = 64854099 OR media_id = 67102704 OR media_id = 67102758 OR media_id = 67102823 OR media_id = 67102923 OR media_id = 67102991 OR media_id = 67409968 "
                + "OR media_id = 67425958 OR media_id = 69581739 OR media_id = 69902352 OR media_id = 77041794 OR media_id = 77042317 OR media_id = 77042419 OR media_id = 77042607 "
                + "OR media_id = 77133328 OR media_id = 77133445 OR media_id = 77133519 OR media_id = 140 OR media_id = 3264387 OR media_id = 3264455 OR media_id = 3264523 "
                + "OR media_id = 65982154 OR media_id = 65982356 OR media_id = 141 OR media_id = 58721638 OR media_id = 59698851 OR media_id = 59698923 OR media_id = 59805097 "
                + "OR media_id = 59805166 OR media_id = 59805235 OR media_id = 59805304 OR media_id = 66225057 OR media_id = 71930557 OR media_id = 71930578 OR media_id = 55097116 "
                + "OR media_id = 55101161 OR media_id = 55103652 OR media_id = 55106387 OR media_id = 62046263 OR media_id = 154 OR media_id = 59738411 OR media_id = 59738458 "
                + "OR media_id = 59738505 OR media_id = 64397729 OR media_id = 64897053 OR media_id = 66485631 OR media_id = 68213411 OR media_id = 23 OR media_id = 24 "
                + "OR media_id = 25 OR media_id = 26 OR media_id = 27 OR media_id = 60505044 OR media_id = 63293532 OR media_id = 68582738 OR media_id = 71916462 "
                + "OR media_id = 63294136 OR media_id = 72 OR media_id = 73 OR media_id = 74 OR media_id = 75 OR media_id = 76 OR media_id = 77 OR media_id = 78 OR media_id = 79 "
                + "OR media_id = 80 OR media_id = 55874320 OR media_id = 59809236 OR media_id = 61011675 OR media_id = 61886419 OR media_id = 64693948 OR media_id = 67322242 "
                + "OR media_id = 68273046 OR media_id = 70072636 OR media_id = 74534152 OR media_id = 76342761 OR media_id = 4294273 OR media_id = 67387111 OR media_id = 60362913 "
                + "OR media_id = 68253459 OR media_id = 60362914 OR media_id = 67820 OR media_id = 64658131 OR media_id = 68384425 OR media_id = 71398673 OR media_id = 62542181 "
                + "OR media_id = 3263119 OR media_id = 59786652 OR media_id = 59786673 OR media_id = 16128 OR media_id = 55831870 OR media_id = 58381550 OR media_id = 59949741 "
                + "OR media_id = 59820961 OR media_id = 60422086 OR media_id = 60422095 OR media_id = 60422104 OR media_id = 65113499 OR media_id = 68280362 OR media_id = 61162521 "
                + "OR media_id = 62827656 OR media_id = 63150848 OR media_id = 63152006 OR media_id = 61190448 OR media_id = 62017376 OR media_id = 64854196 OR media_id = 65709715 "
                + "OR media_id = 68039591 OR media_id = 62149524 OR media_id = 62149535 OR media_id = 62259450 OR media_id = 62259459 OR media_id = 62307645 OR media_id = 62325686 "
                + "OR media_id = 62470947 OR media_id = 62506916 OR media_id = 63528051 OR media_id = 66406223 OR media_id = 66406245 OR media_id = 66406261 OR media_id = 66406271 "
                + "OR media_id = 67529080 OR media_id = 63592565 OR media_id = 67160305 OR media_id = 67160316 OR media_id = 67160341 OR media_id = 67160345 OR media_id = 67160347 "
                + "OR media_id = 67160362 OR media_id = 67274251 OR media_id = 70194680 OR media_id = 63593656 OR media_id = 64854124 OR media_id = 67551125 OR media_id = 67596488 "
                + "OR media_id = 67643586 OR media_id = 67643600 OR media_id = 67643614 OR media_id = 67643628 "
                + "OR media_id = 75213963 OR media_id = 75213996 OR media_id = 61905080 OR media_id = 63817262 OR media_id = 64205490 OR media_id = 64944725 OR media_id = 64944738 "
                + "OR media_id = 64944751 OR media_id = 66378271 "
                + "OR media_id = 66759156 OR media_id = 22 OR media_id = 64572354 OR media_id = 64631762 OR media_id = 75336027 OR media_id = 75336297 OR media_id = 75336471 "
                + "OR media_id = 74807721 OR media_id = 64792267 OR media_id = 64794726 OR media_id = 65450790 OR media_id = 65450809 OR media_id = 65450896 OR media_id = 66333933 "
                + "OR media_id = 64858827 OR media_id = 67565798 OR media_id = 69945741 OR media_id = 73766959 OR media_id = 73767049 OR media_id = 73767115 OR media_id = 73767510 "
                + "OR media_id = 64975618 OR media_id = 75669260 OR media_id = 65040105 OR media_id = 67428493 OR media_id = 65879385 OR media_id = 65196601 OR media_id = 65196604 "
                + "OR media_id = 65196605 OR media_id = 65196606 OR media_id = 68039954 OR media_id = 68040073 OR media_id = 68040115 OR media_id = 65302433 OR media_id = 65850658 "
                + "OR media_id = 65850673 OR media_id = 66274728 OR media_id = 66276361 OR media_id = 66276370 OR media_id = 74834083 OR media_id = 74834171 OR media_id = 74834201 "
                + "OR media_id = 74834241 OR media_id = 74834291 OR media_id = 74834354 OR media_id = 74834395 OR media_id = 74834538 OR media_id = 74834598 OR media_id = 74835532 "
                + "OR media_id = 65337986 OR media_id = 65337988 OR media_id = 65337989 OR media_id = 65337990 OR media_id = 65337991 OR media_id = 65337992 OR media_id = 65337993 "
                + "OR media_id = 65850942 OR media_id = 65881262 OR media_id = 65881267 OR media_id = 65881272 OR media_id = 65881277 OR media_id = 65881282 OR media_id = 65881287 "
                + "OR media_id = 65881292 OR media_id = 65881297 OR media_id = 65881302 OR media_id = 65881307 OR media_id = 65881312 OR media_id = 68310062 OR media_id = 68332854 "
                + "OR media_id = 74911907 OR media_id = 74912040 OR media_id = 65337995 OR media_id = 65337997 OR media_id = 65337998 OR media_id = 65338000 OR media_id = 65338001 "
                + "OR media_id = 65338002 OR media_id = 65338003 OR media_id = 65850988 OR media_id = 65882368 OR media_id = 65882373 OR media_id = 65882378 OR media_id = 65882383 "
                + "OR media_id = 65882388 OR media_id = 65882393 OR media_id = 65882398 OR media_id = 75829398 OR media_id = 75845412 OR media_id = 75845575 OR media_id = 76234605 "
                + "OR media_id = 65338004 OR media_id = 65851648 OR media_id = 65851684 OR media_id = 65851685 OR media_id = 65851686 OR media_id = 65851813 OR media_id = 65851843 "
                + "OR media_id = 65851872 OR media_id = 65851895 OR media_id = 65851917 OR media_id = 65338006 OR media_id = 65338008 OR media_id = 65338010 OR media_id = 65338011 "
                + "OR media_id = 65338012 OR media_id = 65874880 OR media_id = 65874901 OR media_id = 65874977 OR media_id = 65874998 OR media_id = 65875019 OR media_id = 65875040 "
                + "OR media_id = 65875061 OR media_id = 65875082 OR media_id = 65338013 OR media_id = 65338015 OR media_id = 65963730 OR media_id = 65963733 OR media_id = 70583780 "
                + "OR media_id = 65338016 OR media_id = 70685072 OR media_id = 70685076 OR media_id = 70685091 OR media_id = 70685103 OR media_id = 72415535 OR media_id = 72433212 "
                + "OR media_id = 65338020 OR media_id = 65534628 OR media_id = 65554016 "
                + "OR media_id = 65604907 OR media_id = 65338018 OR media_id = 65659673 OR media_id = 72587634 OR media_id = 72605951 OR media_id = 72650996 OR media_id = 74709627 "
                + "OR media_id = 74709807 OR media_id = 65674276 OR media_id = 65736689 OR media_id = 65737363 OR media_id = 65737366 OR media_id = 65737369 OR media_id = 65737372 "
                + "OR media_id = 65737375 OR media_id = 65737378 OR media_id = 65737381 OR media_id = 65737384 OR media_id = 65737387 OR media_id = 65737390 OR media_id = 65773569 "
                + "OR media_id = 65773585 OR media_id = 65773678 OR media_id = 65773714 OR media_id = 65773843 OR media_id = 66083607 OR media_id = 66083608 OR media_id = 66083609 "
                + "OR media_id = 66083622 OR media_id = 66710880 OR media_id = 65707021 OR media_id = 65773893 OR media_id = 65773895 OR media_id = 65773901 OR media_id = 67051047 "
                + "OR media_id = 67051052 OR media_id = 65794928 OR media_id = 65796339 OR media_id = 73908735 OR media_id = 74030147 OR media_id = 65796340 OR media_id = 65796335 "
                + "OR media_id = 65985101 OR media_id = 65985102 OR media_id = 65985103 OR media_id = 76797952 OR media_id = 66024159 OR media_id = 66044592 OR media_id = 66055819 "
                + "OR media_id = 66097555 OR media_id = 66102654 OR media_id = 66137767 OR media_id = 77463679 OR media_id = 77463786 OR media_id = 62677964 OR media_id = 65338022 "
                + "OR media_id = 66251776 OR media_id = 66395527 OR media_id = 66409945 OR media_id = 66409952 OR media_id = 66409953 OR media_id = 71933405 OR media_id = 71933426 "
                + "OR media_id = 71933441 OR media_id = 71933455 OR media_id = 66453829 OR media_id = 66453820 OR media_id = 66453763 OR media_id = 66453803 OR media_id = 66478667 "
                + "OR media_id = 66482243 OR media_id = 66969501 OR media_id = 66969504 OR media_id = 66969507 OR media_id = 66969510 OR media_id = 66969513 OR media_id = 66969516 "
                + "OR media_id = 66969519 OR media_id = 66969522 OR media_id = 66506289 OR media_id = 77375180 OR media_id = 66547927 OR media_id = 67159789 OR media_id = 67159825 "
                + "OR media_id = 67159835 OR media_id = 67159838 OR media_id = 67159871 OR media_id = 67159874 OR media_id = 67159876 OR media_id = 67159898 OR media_id = 67159929 "
                + "OR media_id = 67159938 OR media_id = 67159943 OR media_id = 72637275 OR media_id = 72639549 OR media_id = 72901029 OR media_id = 66580210 OR media_id = 66583633 "
                + "OR media_id = 66584426 OR media_id = 66605440 OR media_id = 67160020 OR media_id = 67160042 OR media_id = 67160071 OR media_id = 67160076 OR media_id = 67160101 "
                + "OR media_id = 67160108 OR media_id = 67160147 OR media_id = 67160158 OR media_id = 67160159 OR media_id = 67160162 OR media_id = 67160188 OR media_id = 67160205 "
                + "OR media_id = 67160246 OR media_id = 70183281 OR media_id = 66633452 OR media_id = 73752125 OR media_id = 73752137 OR media_id = 66768586 OR media_id = 72602322 "
                + "OR media_id = 67736420 OR media_id = 67158341 OR media_id = 67158459 OR media_id = 67158499 OR media_id = 67158504 OR media_id = 67158517 OR media_id = 67158530 "
                + "OR media_id = 67158548 OR media_id = 67158569 OR media_id = 67158574 OR media_id = 67158580 OR media_id = 67158612 OR media_id = 67158613 OR media_id = 67158614 "
                + "OR media_id = 67158618 OR media_id = 67158624 OR media_id = 67158625 OR media_id = 67158626 OR media_id = 67158627 OR media_id = 67158633 OR media_id = 67158634 "
                + "OR media_id = 67158643 OR media_id = 67158658 OR media_id = 70178075 OR media_id = 70178156 OR media_id = 70178579 OR media_id = 70178702 OR media_id = 70178818 "
                + "OR media_id = 70179118 OR media_id = 71308307 OR media_id = 71308315 OR media_id = 71308509 OR media_id = 71308552 OR media_id = 71308608 OR media_id = 71308642 "
                + "OR media_id = 71308657 OR media_id = 71308664 OR media_id = 71308787 OR media_id = 71308856 OR media_id = 71308890 OR media_id = 67158751 OR media_id = 67158782"
                + " OR media_id = 70142883 OR media_id = 67158855 OR media_id = 67158868 OR media_id = 67158897 OR media_id = 67158900 OR media_id = 67158925 OR media_id = 67158933 "
                + "OR media_id = 67158974 OR media_id = 67159044 OR media_id = 67159084 OR media_id = 67159099 OR media_id = 67159100 OR media_id = 67159111 OR media_id = 67159139 "
                + "OR media_id = 67159140 OR media_id = 67159141 OR media_id = 67159193 OR media_id = 67159214 OR media_id = 67159233 OR media_id = 67159273 OR media_id = 67159312 "
                + "OR media_id = 67159341 OR media_id = 67159360 OR media_id = 67159387 OR media_id = 67159394 OR media_id = 67159454 OR media_id = 67159455 OR media_id = 67159477 "
                + "OR media_id = 67159481 OR media_id = 67159498 OR media_id = 67159500 OR media_id = 67159519 OR media_id = 70193971 OR media_id = 70193981 OR media_id = 70194005 "
                + "OR media_id = 70194017 OR media_id = 70194043 OR media_id = 70194050 OR media_id = 67159570 "
                + "OR media_id = 67159594 OR media_id = 67159619 OR media_id = 67159639 OR media_id = 72465152 OR media_id = 67159661 OR media_id = 67159683 OR media_id = 67159690 "
                + "OR media_id = 67159702 OR media_id = 67159703 OR media_id = 67359839 OR media_id = 163 OR media_id = 65337999 OR media_id = 65721879 OR media_id = 66816650 "
                + "OR media_id = 67158392 OR media_id = 67340591 OR media_id = 67354125 OR media_id = 67365772 OR media_id = 68559245 OR media_id = 67431261 OR media_id = 72221593 "
                + "OR media_id = 72221630 "
                + "OR media_id = 67431324 OR media_id = 67431382 OR media_id = 72217459 OR media_id = 72217770 OR media_id = 72218008 OR media_id = 72218102 OR media_id = 72218199 "
                + "OR media_id = 67431437 OR media_id = 67431452 OR media_id = 67431531 OR media_id = 72242828 OR media_id = 72242874 OR media_id = 67431797 OR media_id = 72243527 "
                + "OR media_id = 72243577 OR media_id = 72243629 OR media_id = 72243678 OR media_id = 67431831 "
                + "OR media_id = 72195744 OR media_id = 72195853 OR media_id = 72196028 OR media_id = 72196066 OR media_id = 67461770 OR media_id = 67563780 OR media_id = 67579821 "
                + "OR media_id = 67672792 OR media_id = 67683881 OR media_id = 67693687 OR media_id = 67805875 OR media_id = 67805883 OR media_id = 67805891 OR media_id = 67805899 "
                + "OR media_id = 67805907 OR media_id = 67805915 OR media_id = 67805923 OR media_id = 67805931 OR media_id = 67805939 OR media_id = 67805947 OR media_id = 67805955 "
                + "OR media_id = 67805963 OR media_id = 67805979 OR media_id = 67805987 OR media_id = 67805995 OR media_id = 67806003 OR media_id = 67806019 OR media_id = 67806027 "
                + "OR media_id = 67806035 OR media_id = 67806043 OR media_id = 67806051 OR media_id = 67806059 OR media_id = 67806067 OR media_id = 67806083 OR media_id = 67806091 "
                + "OR media_id = 67806099 OR media_id = 67806107 OR media_id = 67806115 OR media_id = 67806123 OR media_id = 67806143 OR media_id = 67806151 OR media_id = 67806159 "
                + "OR media_id = 67806167 OR media_id = 67806175 OR media_id = 67806183 OR media_id = 67806191 OR media_id = 67806199 OR media_id = 67806207 OR media_id = 67806215 "
                + "OR media_id = 67806223 OR media_id = 67806231 OR media_id = 67806239 OR media_id = 67806255 OR media_id = 67806264 OR media_id = 67806272 OR media_id = 67806280 "
                + "OR media_id = 67806288 OR media_id = 67806296 OR media_id = 67806308 OR media_id = 67806317 OR media_id = 67693692 OR media_id = 68073856 OR media_id = 68073864 "
                + "OR media_id = 68073870 OR media_id = 68073874 OR media_id = 68073878 OR media_id = 68073882 OR media_id = 68073890 OR media_id = 68073894 OR media_id = 68073898 "
                + "OR media_id = 68073906 OR media_id = 68074823 OR media_id = 68074827 OR media_id = 68074831 OR media_id = 68074835 OR media_id = 68074839 OR media_id = 68074843 "
                + "OR media_id = 68074847 OR media_id = 68074851 OR media_id = 68074855 OR media_id = 68074859 OR media_id = 68074863 "
                + "OR media_id = 68074867 OR media_id = 68074875 OR media_id = 68074880 OR media_id = 68074888 OR media_id = 68074892 OR media_id = 68074896 OR media_id = 68074900 "
                + "OR media_id = 68074904 OR media_id = 68074908 OR media_id = 68074916 OR media_id = 68074923 OR media_id = 68074927 OR media_id = 68074931 OR media_id = 68074935 "
                + "OR media_id = 68074939 OR media_id = 68074943 OR media_id = 68074947 OR media_id = 68074952 OR media_id = 68074956 OR media_id = 68074960 OR media_id = 68074964 "
                + "OR media_id = 68074968 OR media_id = 68074972 OR media_id = 68074976 OR media_id = 68074980 OR media_id = 68074984 OR media_id = 68074988 OR media_id = 68074992 "
                + "OR media_id = 68074996 OR media_id = 68075005 OR media_id = 68075009 OR media_id = 68075013 OR media_id = 68075022 OR media_id = 71850153 OR media_id = 71850203 "
                + "OR media_id = 71850253 OR media_id = 71850303 OR media_id = 71850376 OR media_id = 71850426 OR media_id = 71850476 OR media_id = 71850547 OR media_id = 71850615 "
                + "OR media_id = 71850674 OR media_id = 71850724 OR media_id = 71850788 OR media_id = 71850853 OR media_id = 71850903 OR media_id = 71850953 OR media_id = 71851003 "
                + "OR media_id = 71851053 OR media_id = 71851103 OR media_id = 71851166 OR media_id = 71851234 OR media_id = 71851290 OR media_id = 71851346 OR media_id = 71851444 "
                + "OR media_id = 71851514 OR media_id = 71851669 OR media_id = 71851735 OR media_id = 71851799 OR media_id = 74419469 OR media_id = 74419520 OR media_id = 74419571 "
                + "OR media_id = 74419623 OR media_id = 74419676 OR media_id = 67745571 OR media_id = 70191162 OR media_id = 70191169 OR media_id = 70191184 OR media_id = 70191193 "
                + "OR media_id = 70191216 OR media_id = 70191241 OR media_id = 70191270 OR media_id = 70191286 OR media_id = 70191299 OR media_id = 70191345 OR media_id = 70191363 "
                + "OR media_id = 70191402 OR media_id = 70191411 OR media_id = 70191418 OR media_id = 70191436 OR media_id = 70191456 OR media_id = 70191468 OR media_id = 70191479 "
                + "OR media_id = 70191507 OR media_id = 70191527 OR media_id = 70191548 OR media_id = 70191597 OR media_id = 70191604 OR media_id = 70191619 OR media_id = 70191630 "
                + "OR media_id = 70191650 OR media_id = 67776760 OR media_id = 67788426 OR media_id = 67788584 OR media_id = 69879017 OR media_id = 67796331 OR media_id = 67905775 "
                + "OR media_id = 67912168 OR media_id = 67912197 OR media_id = 67912453 OR media_id = 67912471 OR media_id = 67950430 OR media_id = 67978748 OR media_id = 67987385 "
                + "OR media_id = 67999220 OR media_id = 67988000 OR media_id = 67988036 OR media_id = 67988117 OR media_id = 68022716 OR media_id = 68106126 OR media_id = 68073886 "
                + "OR media_id = 68073902 OR media_id = 68074871 OR media_id = 68074912 OR media_id = 68075038 OR media_id = 68074884 OR media_id = 68075026 OR media_id = 68075030 "
                + "OR media_id = 68075043 OR media_id = 68075034 OR media_id = 67805971 OR media_id = 67806011 OR media_id = 67806247 OR media_id = 67806325 OR media_id = 72504923 "
                + "OR media_id = 67806075 OR media_id = 68467080 OR media_id = 68565634 OR media_id = 68573928 OR media_id = 68574573 OR media_id = 68804326 OR media_id = 69109052 "
                + "OR media_id = 69294988 OR media_id = 69401711 OR media_id = 69401814 OR media_id = 69401871 OR media_id = 69500513 OR media_id = 69592775 OR media_id = 69642032 "
                + "OR media_id = 69784290 OR media_id = 70213927 OR media_id = 69843346 OR media_id = 70120660 OR media_id = 70190484 OR media_id = 70347554 OR media_id = 70377409 "
                + "OR media_id = 70613996 OR media_id = 72945395 OR media_id = 72945405 OR media_id = 72945432 OR media_id = 72945479 OR media_id = 72945584 OR media_id = 72945600 "
                + "OR media_id = 72945628 OR media_id = 74978656 OR media_id = 70628267 OR media_id = 70685034 OR media_id = 70718900 OR media_id = 77627190 OR media_id = 71098342 "
                + "OR media_id = 71244998 OR media_id = 73178776 OR media_id = 71316704 OR media_id = 71321832 OR media_id = 71963847 OR media_id = 72040872 OR media_id = 72235782 "
                + "OR media_id = 72235832 OR media_id = 72235917 OR media_id = 71331604 OR media_id = 72375211 OR media_id = 72427386 OR media_id = 72905164 OR media_id = 73761833 "
                + "OR media_id = 72947128 OR media_id = 72947184 OR media_id = 72947201 OR media_id = 72947160 OR media_id = 72947199 OR media_id = 72947395 OR media_id = 72947396 "
                + "OR media_id = 72947400 OR media_id = 72947478 OR media_id = 72947529 OR media_id = 72947530 OR media_id = 72947614 OR media_id = 72947616 OR media_id = 72947624 "
                + "OR media_id = 72947802 OR media_id = 72947809 OR media_id = 72947812 OR media_id = 72996147 OR media_id = 72996162 OR media_id = 73004815 OR media_id = 73061075 "
                + "OR media_id = 73063418 OR media_id = 73079429 OR media_id = 73099344 OR media_id = 73099372 OR media_id = 73099403 OR media_id = 73212162 OR media_id = 73212550 "
                + "OR media_id = 73212799 OR media_id = 73217399 OR media_id = 73223455 OR media_id = 73507212 OR media_id = 74801728 OR media_id = 77812797 OR media_id = 73474405 "
                + "OR media_id = 73474610 OR media_id = 73566981 OR media_id = 73571162 OR media_id = 74084976 OR media_id = 74084991 OR media_id = 74084997 OR media_id = 72079854 "
                + "OR media_id = 73764305 OR media_id = 75284108 OR media_id = 75286077 OR media_id = 75286108 OR media_id = 75286132 OR media_id = 75286173 OR media_id = 75286185 "
                + "OR media_id = 75286211 OR media_id = 75286241 OR media_id = 75286289 OR media_id = 75286344 OR media_id = 75286365 OR media_id = 73838089 OR media_id = 75398136 "
                + "OR media_id = 77423959 OR media_id = 73905637 OR media_id = 76568336 OR media_id = 76568785 OR media_id = 73913296 OR media_id = 74000395 OR media_id = 74002079 "
                + "OR media_id = 74590657 OR media_id = 74590766 "
                + "OR media_id = 74593744 OR media_id = 74593895 OR media_id = 74002394 OR media_id = 74371744 OR media_id = 74000105 OR media_id = 74235244 OR media_id = 75312461 "
                + "OR media_id = 75312570 OR media_id = 75312665 OR media_id = 75313030 OR media_id = 75313046 OR media_id = 75313071 OR media_id = 75313117 OR media_id = 74253324 "
                + "OR media_id = 74253634 OR media_id = 74253677 OR media_id = 74253719 OR media_id = 74253764 OR media_id = 74253806 OR media_id = 74272248 OR media_id = 74419103 "
                + "OR media_id = 77555016 OR media_id = 77555471 OR media_id = 74420758 OR media_id = 74442634 OR media_id = 74490584 OR media_id = 74541955 OR media_id = 74567250 "
                + "OR media_id = 74590739 OR media_id = 75394982 OR media_id = 74052206 OR media_id = 74594869 "
                + "OR media_id = 74595311 OR media_id = 74595403 OR media_id = 74595440 OR media_id = 74595313 OR media_id = 74621788 OR media_id = 74625235 OR media_id = 74630644 "
                + "OR media_id = 74636212 OR media_id = 74636301 OR media_id = 74637427 OR media_id = 74639139 OR media_id = 74639397 OR media_id = 74639579 OR media_id = 74639628 "
                + "OR media_id = 74637660 OR media_id = 74640890 OR media_id = 74642843 OR media_id = 74643363 OR media_id = 74645836 OR media_id = 74659244 OR media_id = 74659428 "
                + "OR media_id = 74659441 OR media_id = 74659467 OR media_id = 74659510 OR media_id = 74659528 OR media_id = 74659534 OR media_id = 74659601 OR media_id = 74659606 "
                + "OR media_id = 74659658 OR media_id = 74659697 OR media_id = 74659748 OR media_id = 74659753 OR media_id = 74659794 OR media_id = 74659873 OR media_id = 74659927 "
                + "OR media_id = 74659960 OR media_id = 74659982 OR media_id = 74659994 OR media_id = 74660030 OR media_id = 74660073 OR media_id = 74661677 OR media_id = 74661691 "
                + "OR media_id = 74661731 OR media_id = 74661751 OR media_id = 74661777 OR media_id = 74661803 OR media_id = 74661815 OR media_id = 74661831 OR media_id = 74661856 "
                + "OR media_id = 74661879 OR media_id = 74661914 OR media_id = 74661923 OR media_id = 74661942 OR media_id = 74661967 OR media_id = 74662001 OR media_id = 74662027 "
                + "OR media_id = 74671215 OR media_id = 74671283 OR media_id = 74683708 OR media_id = 74697880 OR media_id = 74698734 OR media_id = 74698844 OR media_id = 74698880 "
                + "OR media_id = 74698884 OR media_id = 74698773 OR media_id = 74698835 OR media_id = 74698853 OR media_id = 74698903 OR media_id = 74704012 OR media_id = 74708079 "
                + "OR media_id = 74713977 OR media_id = 74739486 OR media_id = 74747655 OR media_id = 74748569 OR media_id = 74749592 OR media_id = 74750408 OR media_id = 74751287 "
                + "OR media_id = 74751817 OR media_id = 74752209 OR media_id = 74752535 OR media_id = 74774067 OR media_id = 74775324 OR media_id = 74776775 OR media_id = 74786859 "
                + "OR media_id = 74790362 OR media_id = 74791637 OR media_id = 74796013 OR media_id = 74798491 OR media_id = 74803881 OR media_id = 74804650 OR media_id = 74805428 "
                + "OR media_id = 74807744 OR media_id = 74818208 OR media_id = 74821348 OR media_id = 74835252 OR media_id = 74837163 OR media_id = 74837615 OR media_id = 74838031 "
                + "OR media_id = 74838310 OR media_id = 74838425 OR media_id = 74838451 OR media_id = 74838452 OR media_id = 74838453 OR media_id = 74838709 OR media_id = 74849263 "
                + "OR media_id = 74850040 OR media_id = 74855872 OR media_id = 74856839 OR media_id = 74858270 OR media_id = 74858333 OR media_id = 74859164 OR media_id = 74861046 "
                + "OR media_id = 74862190 OR media_id = 74863293 OR media_id = 74864241 OR media_id = 74864884 OR media_id = 74866583 OR media_id = 74867270 OR media_id = 74868103 "
                + "OR media_id = 74869043 OR media_id = 74869488 OR media_id = 74881587 OR media_id = 74883901 OR media_id = 74884476 OR media_id = 74886154 OR media_id = 74886732 "
                + "OR media_id = 74887270 OR media_id = 74887374 OR media_id = 74888085 OR media_id = 74888599 OR media_id = 74890653 OR media_id = 74891546 OR media_id = 74909601 "
                + "OR media_id = 74916572 OR media_id = 74916755 OR media_id = 74916901 OR media_id = 77383098 OR media_id = 74917045 OR media_id = 74921211 OR media_id = 74922363 "
                + "OR media_id = 74923870 OR media_id = 74924429 OR media_id = 74924867 OR media_id = 74925875 OR media_id = 74926384 OR media_id = 74926877 OR media_id = 74928050 "
                + "OR media_id = 74932127 OR media_id = 74932703 OR media_id = 74934419 OR media_id = 74936838 OR media_id = 74938660 OR media_id = 74939591 OR media_id = 74941393 "
                + "OR media_id = 74950499 OR media_id = 74962402 OR media_id = 74966064 OR media_id = 74987580 OR media_id = 74988384 OR media_id = 74990935 OR media_id = 74991072 "
                + "OR media_id = 74991777 OR media_id = 74992049 OR media_id = 74997168 OR media_id = 74998045 OR media_id = 75004735 OR media_id = 75006320 OR media_id = 75020595 "
                + "OR media_id = 75023538 OR media_id = 75032719 OR media_id = 75032776 OR media_id = 75032807 OR media_id = 75032831 OR media_id = 75032857 OR media_id = 75032881 "
                + "OR media_id = 75032913 OR media_id = 75032944 OR media_id = 75032990 OR media_id = 75033009 OR media_id = 75033055 OR media_id = 75033102 OR media_id = 75033121 "
                + "OR media_id = 75033162 OR media_id = 75033169 OR media_id = 75033186 OR media_id = 75033209 OR media_id = 75033237 OR media_id = 75033302 OR media_id = 75033346 "
                + "OR media_id = 75311748 OR media_id = 77620747 OR media_id = 75033219 OR media_id = 75035675 OR media_id = 75037348 OR media_id = 75037407 OR media_id = 75037422 "
                + "OR media_id = 75038292 OR media_id = 75038851 OR media_id = 75038866 OR media_id = 75039779 OR media_id = 75043923 OR media_id = 75045300 OR media_id = 75045329 "
                + "OR media_id = 75045336 OR media_id = 75045352 OR media_id = 75045422 OR media_id = 75045843 OR media_id = 75047177 OR media_id = 75047287 OR media_id = 75047316 "
                + "OR media_id = 75047488 OR media_id = 75047499 OR media_id = 75047609 OR media_id = 75047619 OR media_id = 75063268 OR media_id = 75064566 OR media_id = 75067901 "
                + "OR media_id = 75076032 OR media_id = 75077406 OR media_id = 75158180 OR media_id = 75190140 OR media_id = 75223710 OR media_id = 75223721 OR media_id = 75223827 "
                + "OR media_id = 75239526 OR media_id = 75243813 OR media_id = 75245507 OR media_id = 75246495 OR media_id = 75249495 OR media_id = 75254557 OR media_id = 75303535 "
                + "OR media_id = 75317257 OR media_id = 75328026 OR media_id = 75349698 OR media_id = 75350448 OR media_id = 75353536 OR media_id = 75353850 OR media_id = 75354056 "
                + "OR media_id = 75354146 OR media_id = 75354475 OR media_id = 75354540 OR media_id = 75354641 OR media_id = 75354734 OR media_id = 75354824 OR media_id = 75369788 "
                + "OR media_id = 75401129 OR media_id = 75472826 OR media_id = 75611397 OR media_id = 75719017 OR media_id = 75868382 OR media_id = 75877662 OR media_id = 75885329 "
                + "OR media_id = 67158807 OR media_id = 76052084 OR media_id = 75984761 OR media_id = 76061832 OR media_id = 76087186 OR media_id = 76089277 OR media_id = 76100067 "
                + "OR media_id = 76123363 OR media_id = 76135929 OR media_id = 76204709 OR media_id = 76207613 OR media_id = 76209052 OR media_id = 76215744 OR media_id = 76216408 "
                + "OR media_id = 76216699 OR media_id = 76217392 OR media_id = 76225285 OR media_id = 76240138 OR media_id = 76241488 OR media_id = 76247819 OR media_id = 76259765 "
                + "OR media_id = 76268165 OR media_id = 76937651 OR media_id = 76272665 OR media_id = 65963736 OR media_id = 76287940 "
                + "OR media_id = 76289167 OR media_id = 76295833 OR media_id = 76296903 OR media_id = 76297152 OR media_id = 76297195 OR media_id = 76313108 OR media_id = 76318792 "
                + "OR media_id = 76322396 OR media_id = 76323658 OR media_id = 76325936 OR media_id = 76330598 "
                + "OR media_id = 76342816 OR media_id = 76354126 OR media_id = 76355114 OR media_id = 76355957 OR media_id = 76357043 OR media_id = 76358382 OR media_id = 76359221 "
                + "OR media_id = 76359759 OR media_id = 76362377 OR media_id = 76362810 OR media_id = 76369064 OR media_id = 76369473 OR media_id = 76369956 OR media_id = 76370442 "
                + "OR media_id = 77411334 OR media_id = 76644631 OR media_id = 76859109 OR media_id = 76880590 OR media_id = 76984196 OR media_id = 77034877 OR media_id = 77272573 "
                + "OR media_id = 77002118 OR media_id = 77329827 OR media_id = 77398619 OR media_id = 77451493 OR media_id = 77737203 OR media_id = 77738835 OR media_id = 77739241 "
                + "OR media_id = 77739427 OR media_id = 77739505 OR media_id = 77739720 OR media_id = 77739805 OR media_id = 77739886 OR media_id = 77738667 OR media_id = 77740168 "
                + "OR media_id = 77740281 OR media_id = 77740319 OR media_id = 77740346 OR media_id = 77740353 OR media_id = 77740378 OR media_id = 77740404 OR media_id = 77740528 "
                + "OR media_id = 73178594 OR media_id = 74698037 OR media_id = 74698129 OR media_id = 76429778 OR media_id = 76657481 OR media_id = 76657564 OR media_id = 76657614 "
                + "OR media_id = 76657666 OR media_id = 76657718 OR media_id = 76657751 OR media_id = 76657800 OR media_id = 76657895 OR media_id = 77537226 OR media_id = 77561790 "
                + "OR media_id = 77623827 OR media_id = 77667332 OR media_id = 77755797 OR media_id = 77798123 OR media_id = 77831696 OR media_id = 77833893 OR media_id = 77948949 "
                + "OR media_id = 77962336 OR media_id = 77970497 OR media_id = 78178704))";

        private static final String QUERY3_SQL = "disabled_for_upload = false "
                + " AND media_id != -1 AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 "
                + " AND ((media_content_approval_instruction != 3 AND content_approval_media_state != 19 AND content_approval_media_state != 17) "
                + " OR (media_clock_approval_instruction != 31 AND clock_approval_media_state != 41) OR (media_instruction_approval_instruction != 51 "
                + " AND (num_instruction_approval_not_performed > 0 OR num_instruction_rejected > 0)) OR (media_class_approval_instruction != 41 "
                + " AND class_approval_media_state != 71) OR (media_house_nr = '' AND media_house_nr_instruction = 10)) "
                + " AND (content_approval_media_state != 18 "
                + " AND clock_approval_media_state != 42 AND class_approval_media_state != 72 AND num_instruction_rejected = 0 AND qc_approval_media_state != 22 "
                + " AND approval_advert_state != 11) "
                + " AND (aired = false) "
                + " AND (approval_advert_state = 10 OR approval_advert_state = 9) "
                + " AND (NOT (time_delivered <= 0 "
                + " AND stopped = false AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 AND (content_approval_media_state = 19 "
                + " OR content_approval_media_state = 17 OR media_content_approval_instruction != 1) AND (class_approval_media_state = 71 OR media_class_approval_instruction != 40) "
                + " AND (clock_approval_media_state = 41 OR media_clock_approval_instruction != 30) "
                + " AND (media_instruction_approval_instruction != 50 OR (num_instruction_approval_not_performed = 0 AND num_instruction_rejected = 0)) "
                + " AND (approval_advert_state = 10 OR approval_advert_state = 9) AND (media_house_nr != '' OR media_house_nr_instruction = 11)))";

        private final String query;

        QUERY_TYPE(String query) {
            this.query = query;
        }

        public String getSql() {
            return query;
        }
    }

    public int mapSize = 1000000;
    public QUERY_TYPE query = QUERY_TYPE.QUERY1;

    private Random random = new Random();
    private IMap<String, ComplexDomainObject> map;
    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<String, ComplexDomainObject> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < mapSize; i++) {
            ComplexDomainObject value = createQuickSearchObject(i);
            String key = value.getQuickSearchKey();
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    private int randomUpTo14() {
        return random.nextInt(14);
    }

    private long randomLong() {
        return random.nextLong();
    }

    @SuppressWarnings("checkstyle:methodlength")
    public ComplexDomainObject createQuickSearchObject(int id) {
        return new ComplexDomainObject(id, randomLong() * 2,
                "l-name" + randomUpTo14(),
                randomLong() * 3,
                "d-name" + randomUpTo14(),
                randomLong() * 4,
                "b-name" + randomUpTo14(),
                randomLong() * 5,
                "m-name" + randomUpTo14(),
                random.nextInt(4), // media_content_approval_instruction
                randomUpTo14(),
                random.nextInt(32), // media_clock_approval_instruction
                randomUpTo14() * 3,
                random.nextInt(52), // media_instruction_approval_instruction
                10, // media_house_number_instruction
                true,
                randomUpTo14() * 3,
                randomLong() * 3,
                randomLong() * 2,
                randomLong() * 2,
                randomLong() * 2,
                randomLong(),
                "copy_code" + randomUpTo14(),
                random.nextBoolean(), // aired
                randomLong(),
                randomUpTo14(),
                randomLong() * 3,
                randomUpTo14() * 3,
                "title" + randomUpTo14(),
                random.nextBoolean(),
                random.nextBoolean(),
                randomLong() * 2,
                randomLong() * 2,
                randomLong() * 3,
                randomLong() * 3,
                randomUpTo14() * 3,
                randomUpTo14() * 3,
                random.nextBoolean(),
                randomLong() * 3,
                random.nextBoolean(),
                "media_agency_planning" + randomUpTo14(),
                "media_agency_buying" + randomUpTo14(),
                "creative_agency" + randomUpTo14(),
                "production_agency" + randomUpTo14(),
                "post_production_agency" + randomUpTo14(),
                "other_agency" + randomUpTo14(),
                random.nextBoolean(),
                "media_house_nr" + randomUpTo14(),
                "b_p_code" + randomUpTo14(),
                random.nextBoolean(),
                random.nextBoolean(),
                randomLong() * 3,
                randomLong() * 3,
                randomLong() * 2,
                random.nextBoolean(), // inactive
                randomLong() * 2,
                randomUpTo14() * 3,
                randomLong() * 2,
                randomLong() * 3,
                randomLong() * 3,
                randomLong() * 2,
                10, // approval_advert_state
                12, // quality_check_state
                14,
                randomLong() * 3,
                random.nextBoolean(),
                randomLong() * 3,
                randomUpTo14() * 2,
                randomLong() * 2,
                randomLong() * 3,
                randomLong() * 3,
                randomLong() * 2,
                randomLong(),
                randomLong() * 2,
                randomLong() * 3,
                randomLong() * 2,
                random.nextInt(20), // content_approval_media_state
                randomLong() * 2,
                randomLong() * 3,
                randomUpTo14() * 3,
                randomLong() * 3,
                randomLong() * 3,
                randomUpTo14() * 2,
                random.nextInt(42), // clock_approval_media_state
                randomLong() * 3,
                randomUpTo14() * 3,
                randomUpTo14() * 2,
                random.nextInt(4), // num_instruction_rejected
                random.nextInt(2), // num_instruction_approval_not_performed
                randomLong() * 3,
                randomUpTo14() * 2,
                random.nextBoolean(),
                randomUpTo14() * 3);
    }

    @SuppressWarnings("unchecked")
    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.predicate = new SqlPredicate(query.getSql());
    }

    @TimeStep
    public void timeStep(ThreadState state) throws Exception {
        long startTime = System.nanoTime();
        Set<Map.Entry<String, ComplexDomainObject>> entries = map.entrySet(state.predicate);
        long durationNanos = System.nanoTime() - startTime;
        long durationMillis = NANOSECONDS.toMillis(durationNanos);

        throttlingLogger.log(INFO, "Query Evaluation Took " + durationMillis + "ms. Size of the result size: " + entries.size());
    }

    public class ThreadState extends BaseThreadState {
        private Predicate<String, ComplexDomainObject> predicate;
    }

}
