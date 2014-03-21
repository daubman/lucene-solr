package org.apache.solr.handler.component;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.search.Query;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class QueryComponentTest extends SolrTestCaseJ4 {

  private final List<String> fqs;
  private final DocSet ds;

  public QueryComponentTest() {
    fqs = Arrays.asList("id:[2 TO 5]", "inStock_b1:true");
    ds = new BitDocSet();
    ds.add(0);
    ds.add(1);
    ds.add(3);
  }


  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    assertU(adoc("id", "1", "title", "this is a title.", "inStock_b1", "true"));
    assertU(adoc("id", "2", "title", "this is another title.", "inStock_b1", "true"));
    assertU(adoc("id", "3", "title", "Mary had a little lamb.", "inStock_b1", "false"));
    assertU(adoc("id", "4", "title", "this is another title that is in stock", "inStock_b1", "true"));
    assertU(adoc("id", "5", "title", "this is yet another title that is also in stock", "inStock_b1", "true"));
    assertU(commit());
  }

  private static String assertJ(ResponseBuilder rb, String... tests) throws Exception {
    SolrQueryResponse rsp = rb.rsp;
    SolrQueryRequest req = rb.req;
    SolrCore core = h.getCoreInc();
    QueryResponseWriter responseWriter = core.getQueryResponseWriter(rb.req);
    StringWriter sw = new StringWriter(32000);
    responseWriter.write(sw, req, rsp);
    String response = sw.toString();
    rb.req.close();
    core.close();

    //System.out.println(sw.toString());

    for (String test : tests) {
      if (test == null || test.length() == 0) continue;
      String testJSON = json(test);

      boolean failed = true;
      try {
        failed = true;
        String err = JSONTestUtil.match(response, testJSON, 0d);
        failed = false;
        if (err != null) {
          log.error("query failed JSON validation. error=" + err +
                  "\n expected =" + testJSON +
                  "\n response = " + response +
                  "\n request = " + req.getParamString()
          );
          throw new RuntimeException(err);
        }
      } finally {
        if (failed) {
          log.error("JSON query validation threw an exception." +
                  "\n expected =" + testJSON +
                  "\n response = " + response +
                  "\n request = " + req.getParamString()
          );
        }
      }
    }
    return response;
  }

  @Test
  public void testPrepareNoFilters() throws Exception {
    QueryComponent component = new QueryComponent();
    List<SearchComponent> components = new ArrayList<>(1);
    components.add(component);
    SolrQueryRequest req;
    ResponseBuilder rb;

    req = req("q", "*:*", "wt", "json", "indent", "true");
    rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
    component.prepare(rb);
    component.process(rb);
    assertJ(rb, "/response/numFound==5");
  }

  @Test
  public void testPrepareOnlySetFilters() throws Exception {
    QueryComponent component = new QueryComponent();
    List<SearchComponent> components = new ArrayList<>(1);
    components.add(component);
    SolrQueryRequest req;
    ResponseBuilder rb;

    req = req("q", "*:*", "wt", "json", "indent", "true");
    rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
    List<Query> qfl = new ArrayList<>(2);
    for (String fq : fqs) {
      QParser fqp = QParser.getParser(fq, null, req);
      qfl.add(fqp.getQuery());
    }
    rb.setFilters(qfl);
    component.prepare(rb);
    component.process(rb);

    assertJ(rb, "/response/numFound==3", "/response/docs/[0]/id==2", "response/docs/[1]/id==4", "response/docs/[2]/id==5");
  }

  @Test
  public void testPrepareOnlySetFilter() throws Exception {
    QueryComponent component = new QueryComponent();
    List<SearchComponent> components = new ArrayList<>(1);
    components.add(component);
    SolrQueryRequest req;
    ResponseBuilder rb;

    req = req("q", "*:*", "wt", "json", "indent", "true");
    rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
    rb.setFilter(ds);
    component.prepare(rb);
    component.process(rb);

    assertJ(rb, "/response/numFound==3", "/response/docs/[0]/id==1", "/response/docs/[1]/id==2", "/response/docs/[2]/id==4");
  }

  @Test
  public void testPrepareSetFilterWithSetFilters() throws Exception {
    QueryComponent component = new QueryComponent();
    List<SearchComponent> components = new ArrayList<>(1);
    components.add(component);
    SolrQueryRequest req;
    ResponseBuilder rb;

    req = req("q", "*:*", "wt", "json", "indent", "true");
    rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
    List<Query> qfl = new ArrayList<>(2);
    for (String fq : fqs) {
      QParser fqp = QParser.getParser(fq, null, req);
      qfl.add(fqp.getQuery());
    }
    rb.setFilter(ds);
    rb.setFilters(qfl);
    component.prepare(rb);
    component.process(rb);

    assertJ(rb, "/response/numFound==2", "/response/docs/[0]/id==2", "response/docs/[1]/id==4");
  }
}
