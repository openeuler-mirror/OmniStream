#include "org_apache_flink_LowerCaseHelper.h"
#include <jni.h>
#include <string>
#include <vector>
#include <cctype>
#include <iostream>
#include <cstdint>
#include <array>
#include <arm_sve.h>
#include "lower_case_mappings.h"


static uint32_t CON[65536];

static void initTable() {
    for (uint32_t i = 0; i < 65536; ++i) {
        CON[i] = i;
    }
    for (const CaseMapping& m : CASE_MAPPINGS) {
        CON[m.from] = m.to;
    }
}

jint JNI_OnLoad(JavaVM *vm, void *reserved) {
    initTable();
    return JNI_VERSION_1_8;
}

void handle_group(uint16_t *data, int page_index, svbool_t pg, svuint16_t v) {
    svbool_t start_192 = svcmpge_n_u16(pg, v, (uint16_t) 192);
    svbool_t end_4301 = svcmple_n_u16(pg, v, (uint16_t) 4301);
    svbool_t lookup_mask = svand_b_z(pg, start_192, end_4301);


    svbool_t start_7680 = svcmpge_n_u16(pg, v, (uint16_t) 7680);
    svbool_t end_11506 = svcmple_n_u16(pg, v, (uint16_t) 11506);
    lookup_mask = svorr_b_z(pg, lookup_mask, svand_b_z(pg, start_7680, end_11506));


    svbool_t start_42560 = svcmpge_n_u16(pg, v, (uint16_t) 42560);
    svbool_t end_42922 = svcmple_n_u16(pg, v, (uint16_t) 42922);
    lookup_mask = svorr_b_z(pg, lookup_mask, svand_b_z(pg, start_42560, end_42922));


    svbool_t start_65313 = svcmpge_n_u16(pg, v, (uint16_t) 65313);
    svbool_t end_65338 = svcmple_n_u16(pg, v, (uint16_t) 65338);
    lookup_mask = svorr_b_z(pg, lookup_mask, svand_b_z(pg, start_65313, end_65338));


    int count = svcntp_b16(pg, lookup_mask);
    if (count > 0) {
        uint16_t a_mask_65338[svcntp_b16(pg, pg)];
        svst1_u16(pg, a_mask_65338, svdup_u16_z(lookup_mask, 1));
        for (int k = 0; k < svcntp_b16(pg, pg); k++) {
            if (a_mask_65338[k] == 1) {
                data[page_index + k] = CON[data[page_index + k]];
            }
        }
    }
}

void lower_sve_u16(uint16_t *data, int len) {

    int i = 0;
    int vl = svcnth();
    while (i < len) {
        // predicate：有效元素
        svbool_t pg = svwhilelt_b16(i, len);

        // load
        svuint16_t v = svld1_u16(pg, &data[i]);

        // 'A' <= c
        svbool_t geA = svcmpge_n_u16(pg, v, 'A');

        // c <= 'Z'
        svbool_t leZ = svcmple_n_u16(pg, v, 'Z');

        // A..Z mask
        svbool_t mask = svand_b_z(pg, geA, leZ);

        // +32
        svuint16_t v_lower = svadd_n_u16_z(mask, v, 32);

        // blend
        v = svsel(mask, v_lower, v);

        // store
        svst1_u16(pg, &data[i], v);

        if (svcntp_b16(pg, mask) == (((len - i) <= vl) ? len - i : vl)) {
            i += vl;
            continue;
        }
        handle_group(data, i, pg, v);
        i += vl;
    }
}


void lower_sve_u8(uint8_t *data, int len) {
    int i = 0;
    int vl = svcntb();
    while (i < len) {
        // predicate：有效元素
        svbool_t pg = svwhilelt_b8(i, len);

        // load
        svuint8_t v = svld1_u8(pg, &data[i]);

        svbool_t geA = svcmpge_n_u8(pg, v, 'A');
        svbool_t leZ = svcmple_n_u8(pg, v, 'Z');
        svbool_t mask = svand_b_z(pg, geA, leZ);

        svbool_t ge192 = svcmpge_n_u8(pg, v, 192);
        svbool_t le222 = svcmple_n_u8(pg, v, 222);
        mask = svorr_b_z(pg,mask,svand_b_z(pg, ge192, le222));
        // +32
        svuint8_t v_lower = svadd_n_u8_z(mask, v, 32);
        // blend
        v = svsel(mask, v_lower, v);
        // store
        svst1_u8(pg, &data[i], v);
        i += vl;
    }
}

JNIEXPORT void JNICALL Java_org_apache_flink_LowerCaseHelper_nativeLower
        (JNIEnv *env, jclass cls, jcharArray valueArray, jbyte coder) {
    int c = coder;
    jsize len = env->GetArrayLength(valueArray);

    jchar *data =
            (jchar *) env->GetPrimitiveArrayCritical(valueArray, NULL);
    lower_sve_u16((uint16_t *) data, len);
    env->ReleasePrimitiveArrayCritical(valueArray, data, 0);
    }
