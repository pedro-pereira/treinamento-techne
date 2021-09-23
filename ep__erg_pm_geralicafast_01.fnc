-- 
-- Tarefa 123456 - 05/08/2021 - Pedro.Pereira - v1.00
-- Alteração Martônio
-- Tarefa Teste Pedro Montenegro - 20/08/2021
-- ALTERADO POR SOLON
-- daniel teste dia 23/09/2021
CREATE FUNCTION EP__ERG_PM_GERALICAFAST_01(P_NUMFUNC  date,
                                            P_RESULT   ERG_PM_RESULTPRONT%ROWTYPE,
                                            P_DECISAO  ERG_PM_DECISAO%ROWTYPE,
                                            P_CONTERRO IN OUT NUMBER,
                                            P_CONCLUIR OUT NUMBER)
RETURN BOOLEAN
IS
  v_dtiniproxlic    date;
  v_diainilic       date;
  v_dias_acumulados number;
  v_dias            number;
  v_diaslic         number;
  v_max             number;
  v_cod             number;
  v_erro            varchar2(4000);
  v_erro_7517       boolean;
  v_diasafast       number;
  v_ep 				      boolean;
  v_conterro 		    number :=50;

  --procura licenças concedidas anteriormente na perícia inicial e prorrogações, além dos dias 
  CURSOR c_lic_ini_pror IS
    SELECT l.dtini, l.dtfim, (l.dtfim-l.dtini+1) as numdias, l.codfreq
      FROM lic_afast l, erg_pm_freq_decisao f
     WHERE l.numfunc = p_numfunc and
           l.numvinc = p_result.numvinc and
           l.codfreq = f.codfreq and
           l.tipofreq = f.tipofreq and
           f.decisao = p_decisao.decisao and
           l.resultpront is not null and
           exists (select 1
                   from prontuario p,
                        erg_pm_resultpront r
                   where p.chave = r.chavepront
                     and (p.chave = P_RESULT.chavepront or 
                          p.chaveant IN (SELECT P2.CHAVEANT
                                         FROM PRONTUARIO P2
                                         WHERE P2.CHAVE = P_RESULT.chavepront) or
                          p.chave IN (SELECT P2.CHAVEANT
                                         FROM PRONTUARIO P2
                                         WHERE P2.CHAVE = P_RESULT.chavepront))
                     and r.resultpront = l.resultpront)
    UNION ALL
    SELECT r.dtini, r.dtfim, (r.dtfim-r.dtini+1) as numdias, (SELECT CODFREQ
                                                              FROM    
                                                               (SELECT FD.CODFREQ
                                                                FROM ERG_PM_FREQ_DECISAO FD,
                                                                     ERG_PM_DECISAO D2,
                                                                     PRONTUARIO P2
                                                                WHERE FD.DECISAO = D2.DECISAO
                                                                  AND D2.SIGLAEXAME = P2.SIGLA
                                                                  AND P2.CHAVE = P_RESULT.chavepront
                                                                ORDER BY FD.DIASAFAST)
                                                              WHERE ROWNUM = 1) CODFREQ
    FROM ERG_PM_RESULTPRONT R, ERG_PM_DECISAO D, PRONTUARIO P
    WHERE R.DECISAO = D.DECISAO
      AND NVL(D.FLEX_CAMPO_02,'N') = 'S'
      AND R.CHAVEPRONT = P.CHAVE
      AND (P.CHAVE = P_RESULT.chavepront OR 
           P.CHAVEANT IN (SELECT P2.CHAVEANT
                          FROM PRONTUARIO P2
                          WHERE P2.CHAVE = P_RESULT.chavepront) OR
           P.CHAVE IN (SELECT P2.CHAVEANT
                       FROM PRONTUARIO P2
                       WHERE P2.CHAVE = P_RESULT.chavepront))
      AND NOT EXISTS (SELECT 1
                      FROM ERG_PM_RESULTPRONT R2
                      WHERE R2.RESULTRETIF = R.RESULTPRONT)
                   
    ORDER BY dtini DESC;

  --procura licenças concedidas anteriormente nos códigos de frequencia da decisão informada
  CURSOR c_licencas IS
    SELECT l.dtini, l.dtfim, (l.dtfim-l.dtini+1) as numdias, l.codfreq
      FROM lic_afast l, erg_pm_freq_decisao f
     WHERE l.numfunc = p_numfunc and
           l.numvinc = p_result.numvinc and
           l.codfreq = f.codfreq and
           l.tipofreq = f.tipofreq and
           f.decisao = p_decisao.decisao
    ORDER BY dtini DESC;
  --
  --faixas de dias para determinar codigo de frequencia
  CURSOR c_faixas is
    SELECT diasafast, codfreq, tipofreq
      FROM erg_pm_freq_decisao
      WHERE decisao = p_decisao.decisao
    ORDER BY diasafast;
  --
  -- procedimento para inserir em lic_afast, salvando os erros ocorridos sem interromper a execução
  --
  PROCEDURE INS_LICAFAST (p_dtini DATE, p_dias NUMBER, p_codfreq NUMBER, p_tipofreq VARCHAR2)
  IS
    v_pont            had_publx.pont%type;
    V_EMPCOD_ORIG NUMBER;
    V_EMPCOD_VINC NUMBER;
  BEGIN
    V_EMPCOD_ORIG := FLAG_PACK.GET_EMPRESA;
    V_EMPCOD_VINC := PACK_ERGON.GET_EMPRESA_FUNC(P_NUMFUNC, P_RESULT.NUMVINC);

    IF V_EMPCOD_VINC = V_EMPCOD_ORIG THEN
      NULL;
    ELSE
      FLAG_PACK.SET_EMPRESA(V_EMPCOD_VINC);
    END IF;


    -- TAREFA 43611: COPIAR AS PUBLICAÇÕES PARA A TABELA DE LICAFAST
    IF p_result.pontpubl is null THEN
      v_pont := NULL;
    ELSE
      SELECT HAD_PUBLX_SEQ.NEXTVAL INTO v_pont FROM DUAL;

      INSERT INTO HAD_PUBLX (PONT, TABELA, NUMFUNC, NUMVINC)
      VALUES (v_pont, 'LIC_AFAST', p_numfunc, p_result.numvinc);

      insert into had_publicacoes
            (pont, versao, motivo, datapubl, tipopubl, numpubl,
             autoridade, tipodo, datado, obs, numero_processo)
      select v_pont, versao, motivo, datapubl, tipopubl, numpubl,
             autoridade, tipodo, datado, obs, numero_processo
      from had_publicacoes
      where (pont = p_result.pontpubl or flex_campo_01 = to_char(p_result.resultpront)); -- 74529
    END IF;
    -- FIM DA TAREFA 43611
    --
    IF P_RESULT.DTFIM IS NOT NULL THEN
      INSERT INTO LIC_AFAST
        (NUMFUNC, NUMVINC, DTINI, DTFIM, TIPOFREQ,
         CODFREQ, MOTIVO,
         DTPREVFIM, EMP_CODIGO,RESULTPRONT, PONTPUBL)
      VALUES
        (P_NUMFUNC, P_RESULT.NUMVINC, p_dtini, p_dtini+p_dias-1, p_tipofreq,
         p_CODFREQ, 'Gerado por perícia médica, laudo número: '||P_RESULT.RESULTPRONT ,
         p_dtini+p_dias-1, FLAG_PACK.GET_EMPRESA, P_RESULT.RESULTPRONT, V_PONT);
    ELSE
      INSERT INTO LIC_AFAST
        (NUMFUNC, NUMVINC, DTINI, DTFIM, TIPOFREQ,
         CODFREQ, MOTIVO,
         DTPREVFIM, EMP_CODIGO,RESULTPRONT, PONTPUBL)
      VALUES
        (P_NUMFUNC, P_RESULT.NUMVINC, p_dtini, NULL, p_tipofreq,
         p_CODFREQ, 'Gerado por perícia médica, laudo número: '||P_RESULT.RESULTPRONT ,
         NULL, FLAG_PACK.GET_EMPRESA, P_RESULT.RESULTPRONT, V_PONT);    
    END IF;

    IF V_EMPCOD_VINC = V_EMPCOD_ORIG THEN
      NULL;
    ELSE
      FLAG_PACK.SET_EMPRESA(V_EMPCOD_ORIG);
    END IF;

  EXCEPTION WHEN OTHERS THEN
    v_erro := SQLERRM;
    v_cod := SQLCODE;
    INSERT INTO erg_pm_erroconcl (resultpront, msgerro)
    VALUES (P_RESULT.RESULTPRONT, ERG_PM_WARN_PROC (v_cod, v_erro));
    p_conterro := p_conterro + 1;

     IF V_EMPCOD_VINC = V_EMPCOD_ORIG THEN
       NULL;
     ELSE
       FLAG_PACK.SET_EMPRESA(V_EMPCOD_ORIG);
     END IF;

  END;-- ins_afast
BEGIN
  --Se gera LIC_AFAST e é APOSENTADORIA POR INVLIDEZ então LIC_AFAST.DTFIM não é obrigatório
  IF NVL(P_DECISAO.GERA_LICAFAST, 'N') = 'S' OR NVL(P_DECISAO.FLEX_CAMPO_01, 'N') = 'S' THEN -- Tarefa 102554
    v_erro_7517 := false;  -- tarefa 48046
    --- Tarefa 100943
    p_conterro:= NVL(p_conterro,0);
    --
    -- quando a decisão pode gerar múltiplos registros em lic_afast, dependendo do número de dias concedido, precisa
    -- fazer a contagem dos dias acumulados de licenças anteriores
    -- O campo INTERVALO_DIAS da tabela Decisão indica o intervalo para zerar a contagem.
    -- se ele não for informado, é porque a decisão não vai gerar múltiplos registros, então não precisa contar acumulados.
    --
    v_dias_acumulados := 0;
    --
    -- faz a contagem de dias acumulados quando necessário, senão fica com zero mesmo.
    IF nvl(p_decisao.intervalo_dias,0) > 0 THEN
      v_dtiniproxlic := p_result.dtini;
      FOR v_lic in c_licencas LOOP
        IF v_dtiniproxlic - v_lic.dtfim > p_decisao.intervalo_dias THEN
          EXIT;
        ELSE
          v_dias_acumulados := v_dias_acumulados + v_lic.numdias;
          v_dtiniproxlic := v_lic.dtini;
        END IF;
        --
        IF v_lic.dtini > p_result.dtini THEN
          ERGON_ERRO_PACK.TRATA_ERRO(7512, p_result.codfreq, p_result.dtini, p_result.dtfim,
                                            V_lic.codfreq, v_lic.dtini, v_lic.dtfim, p_decisao.sigla);
        END IF;
      END LOOP;
    END IF; -- contagem de dias acumulados
    --
    -- passa pelas faixas gerando dias de licença em cada faixa
    v_diaslic := p_result.numdias;
    v_diainilic := p_result.dtini;
    v_max := 0;
    --
    FOR v_faixas in c_faixas LOOP
      -- a licença cabe completamente nessa faixa
      IF (v_dias_acumulados+v_diaslic) <= v_faixas.diasafast OR v_faixas.diasafast is NULL THEN
        ins_licafast (v_diainilic, v_diaslic, v_faixas.CODFREQ, v_faixas.tipofreq);
        v_diaslic := 0; -- acabou.
      --
      -- ainda tem dias para usar nessa faixa, mas não cabe tudo
      -- Tarefa 46369 - Inicio
      ELSIF v_dias_acumulados < v_faixas.diasafast THEN
      -- Tarefa 46369 - Fim
        v_dias := v_faixas.diasafast-v_dias_acumulados;
        ins_licafast (v_diainilic, v_dias, v_faixas.CODFREQ, v_faixas.tipofreq);
      --
        v_diaslic := v_diaslic - v_dias;
        v_dias_acumulados := v_dias_acumulados + v_dias;
        v_diainilic := v_diainilic + v_dias;
      END IF;
      --
      v_max := v_max + nvl(v_faixas.diasafast,0); -- acumula total de dias permitido nas faixas para msg de erro.
      --
      IF v_diaslic = 0 THEN
        EXIT;
      END IF;
    END LOOP;  -- faixas de codigo
    --
    -- verifica se não ficou nenhum dia de licença fora das faixas
    IF v_diaslic > 0 THEN
      ERGON_ERRO_PACK.TRATA_ERRO(7513, p_result.resultpront, p_decisao.sigla, v_max, p_result.numdias);
    END IF;
    --
    RETURN FALSE;
  ELSE
    
    p_concluir := 1;
    --- Tarefa 100943
    p_conterro:=NVL(p_conterro,0);
    --
    -- quando a decisão pode gerar múltiplos registros em lic_afast, dependendo do número de dias concedido, precisa
    -- fazer a contagem dos dias acumulados de licenças anteriores
    -- O campo INTERVALO_DIAS da tabela Decisão indica o intervalo para zerar a contagem.
    -- se ele não for informado, é porque a decisão não vai gerar múltiplos registros, então não precisa contar acumulados.
    --
    v_dias_acumulados := 0;
    --
    -- faz a contagem de dias acumulados quando necessário, senão fica com zero mesmo.
    IF nvl(p_decisao.intervalo_dias,0) > 0 THEN
      v_dtiniproxlic := p_result.dtini;
      FOR v_lic in c_lic_ini_pror LOOP
        IF v_dtiniproxlic - v_lic.dtfim > p_decisao.intervalo_dias THEN
          EXIT;
        ELSE
          v_dias_acumulados := v_dias_acumulados + v_lic.numdias;
          v_dtiniproxlic := v_lic.dtini;
        END IF;
        --
        IF v_lic.dtini > p_result.dtini THEN
          ERGON_ERRO_PACK.TRATA_ERRO(7512, p_result.codfreq, p_result.dtini, p_result.dtfim,
                                     V_lic.codfreq, v_lic.dtini, v_lic.dtfim, p_decisao.sigla);
        END IF;
      END LOOP;
    END IF; -- contagem de dias acumulados
    --
    -- passa pelas faixas gerando dias de licença em cada faixa
    v_diaslic := p_result.numdias;
    v_diainilic := p_result.dtini;
    v_max := 0;
    --
    FOR v_faixas in c_faixas LOOP
      -- Tarefa 50623 início
      /*
             -- Tarefa 48046 início
             v_diasafast := v_faixas.diasafast;
             if (v_dias_acumulados+v_diaslic) > v_faixas.diasafast then
               v_erro_7517 := true;
               EXIT;
             end if;
             -- Tarefa 48046 fim
        */
      -- Tarefa 50623 fim

      -- a licença cabe completamente nessa faixa
      IF (v_dias_acumulados+v_diaslic) <= v_faixas.diasafast OR v_faixas.diasafast is NULL THEN
        ins_licafast (v_diainilic, v_diaslic, v_faixas.CODFREQ, v_faixas.tipofreq);
        v_diaslic := 0; -- acabou.
      --
      -- ainda tem dias para usar nessa faixa, mas não cabe tudo
      -- Tarefa 46369 - Inicio
      ELSIF v_dias_acumulados < v_faixas.diasafast THEN
      -- ELSIF v_dias_acumulados <= v_faixas.diasafast THEN
      -- Tarefa 46369 - Fim
        v_dias := v_faixas.diasafast-v_dias_acumulados;
        ins_licafast (v_diainilic, v_dias, v_faixas.CODFREQ, v_faixas.tipofreq);
      --
        v_diaslic := v_diaslic - v_dias;
        v_dias_acumulados := v_dias_acumulados + v_dias;
        v_diainilic := v_diainilic + v_dias;
      END IF;
      --
      v_max := v_max + nvl(v_faixas.diasafast,0); -- acumula total de dias permitido nas faixas para msg de erro.
      --
      IF v_diaslic = 0 THEN
        EXIT;
      END IF;
    END LOOP;  -- faixas de codigo
    --
    -- Tarefa 50623 início
    /*
        -- Tarefa 48046
        if v_erro_7517 then
            ERGON_ERRO_PACK.TRATA_ERRO(7517, p_result.resultpront, ((v_dias_acumulados+v_diaslic)-v_diasafast), v_diasafast);
        end if;
      */
    -- Tarefa 50623 fim
    --
    -- verifica se não ficou nenhum dia de licença fora das faixas
    IF v_diaslic > 0 THEN
      ERGON_ERRO_PACK.TRATA_ERRO(7513, p_result.resultpront, p_decisao.sigla, v_max, p_result.numdias);
    END IF;
    --
    RETURN FALSE;
  END IF;
EXCEPTION WHEN OTHERS THEN
  v_erro := SQLERRM;
  v_cod := SQLCODE;
  INSERT INTO erg_pm_erroconcl (resultpront, msgerro)
  VALUES (P_RESULT.RESULTPRONT, ERG_PM_WARN_PROC (v_cod, v_erro));
  p_conterro := p_conterro + 1;
  RETURN FALSE;
END;
/
-- INI ---------------------------- HAD_FIXES ---------------------------------------------
INSERT INTO HAD_FIX
  (IDENT, DESCRICAO, DATAALTERACAO, SIS, TIPOOBJ, OBJETO, VERSAO)
VALUES
  ('TAREFA102554', 'Correção da condição para juntar períodos de licenças consecutivas.',
   TO_DATE('05/08/2021', 'DD/MM/YYYY'), 'C_ERGON_PMCG', 'FUNCTION', 'EP__ERG_PM_GERALICAFAST_01', '1.00')
/
COMMIT
/
-- FIM ---------------------------- HAD_FIXES ---------------------------------------------
